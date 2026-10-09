import assert from "node:assert/strict";
import test from "node:test";
import { generateKeyPairSync, sign } from "node:crypto";
import { createServer } from "node:http";
import { promises as fs } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import UnsApiProxy from "@uns-kit/api/uns-api-proxy.js";
import { LegacyImportManager } from "../src/legacy-import.js";
import {
  legacyImportApi,
  importCommandSchema,
} from "../src/legacy-import-api.js";

test("real SDK HTTP/JWKS protects import commands and serves cached status during a pending write", async (t) => {
  const root = await fs.realpath(
    await fs.mkdtemp(path.join(tmpdir(), "legacy-http-test-")),
  );
  const source = path.join(root, "old");
  const live = path.join(root, "live");
  await fs.mkdir(source);
  await fs.mkdir(live);
  await fs.writeFile(
    path.join(source, "a.event"),
    JSON.stringify({ topic: "plant/retired/speed", message: "{}" }),
  );
  const { publicKey, privateKey } = generateKeyPairSync("rsa", {
    modulusLength: 2048,
  });
  const jwk = {
    ...publicKey.export({ format: "jwk" }),
    kid: "local-test-key",
    alg: "RS256",
    use: "sig",
  };
  const jwks = createServer((_req, res) => {
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ keys: [jwk] }));
  });
  await new Promise<void>((resolve) => jwks.listen(0, "127.0.0.1", resolve));
  const jwksPort = (jwks.address() as any).port;
  let finish!: () => void;
  let entered!: () => void;
  const pending = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const entry = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const manager = new LegacyImportManager({
    sources: [{ id: "old", directory: source }],
    liveDirectory: live,
    instanceId: "local-http-test",
    policyDigest: () => "policy",
    canWrite: () => true,
    hasLiveHeadroom: () => true,
    write: async () => {
      entered();
      await pending;
      return { outcome: "written" };
    },
  });
  const api = new UnsApiProxy("archiver-http-test", "import-test", {
    jwks: { wellKnownJwksUrl: `http://127.0.0.1:${jwksPort}/jwks` },
    publishedApiHost: "127.0.0.1",
  });
  const server = (api as any).app.server;
  t.after(async () => {
    finish();
    await manager.close();
    await api.stop();
    server.closeAllConnections();
    jwks.closeAllConnections();
    await Promise.all([
      new Promise<void>((resolve) => server.close(resolve)),
      new Promise<void>((resolve) => jwks.close(() => resolve())),
    ]);
    await fs.rm(root, { recursive: true, force: true });
  });
  const handlers = legacyImportApi(manager);
  api.event.on("apiGetEvent", handlers.get);
  api.event.on("apiPostEvent", handlers.post);
  await api.get("system/", "archiver", "service", "local", "imports");
  await api.post("system/", "archiver", "service", "local", "import-control", {
    requestBody: { required: true, schema: importCommandSchema },
  });
  const base = `http://127.0.0.1:${server.address().port}/api/system/archiver/service/local`;
  const token = (rules: string[]) => {
    const header = Buffer.from(
      JSON.stringify({ alg: "RS256", typ: "JWT", kid: "local-test-key" }),
    ).toString("base64url");
    const payload = Buffer.from(
      JSON.stringify({
        sub: "local-test-operator",
        accessRules: rules,
        exp: Math.floor(Date.now() / 1000) + 60,
      }),
    ).toString("base64url");
    const input = `${header}.${payload}`;
    return `${input}.${sign("RSA-SHA256", Buffer.from(input), privateKey).toString("base64url")}`;
  };
  const admin = token(["/system/archiver/service/local/#"]);
  const reader = token(["/system/archiver/service/local/imports"]);
  const request = (attribute: string, bearer?: string, body?: unknown) =>
    fetch(`${base}/${attribute}`, {
      method: body ? "POST" : "GET",
      headers: {
        ...(bearer ? { Authorization: `Bearer ${bearer}` } : {}),
        "Content-Type": "application/json",
      },
      ...(body ? { body: JSON.stringify(body) } : {}),
      signal: AbortSignal.timeout(3000),
    });
  const start = {
    action: "start",
    sourceId: "old",
    requestId: "start-test",
    expectedRevision: 0,
    confirmSourceClosed: true,
  };
  assert.equal((await request("imports")).status, 401);
  assert.equal((await request("import-control", undefined, start)).status, 401);
  assert.equal(
    (await request("import-control", `${admin}tampered`, start)).status,
    401,
  );
  assert.equal((await request("imports", token(["/unrelated/#"]))).status, 403);
  assert.equal((await request("imports", reader)).status, 200);
  assert.equal((await request("import-control", reader, start)).status, 403);
  const arbitrary = await request("import-control", admin, {
    ...start,
    directory: "/untrusted",
  });
  assert.equal(arbitrary.status, 400);
  assert.deepEqual(await arbitrary.json(), { error: "invalid-command" });
  assert.equal(
    (
      await request("import-control", admin, {
        ...start,
        confirmSourceClosed: false,
      })
    ).status,
    400,
  );
  const started = await request("import-control", admin, start);
  assert.equal(started.status, 200);
  assert.equal(((await started.json()) as any).revision, 1);
  assert.equal((await request("import-control", admin, start)).status, 200);
  const running = manager.tick();
  await entry;
  const status = await request("imports", reader);
  assert.equal(status.status, 200);
  const snapshot = (await status.json()) as any[];
  assert.equal(snapshot[0].job.inFlight, 1);
  assert.ok(!JSON.stringify(snapshot).includes(source));
  const paused = await request("import-control", admin, {
    action: "pause",
    sourceId: "old",
    requestId: "pause-test",
    expectedRevision: 1,
  });
  assert.equal(paused.status, 200);
  assert.equal(((await paused.json()) as any).state, "paused");
  assert.ok(await fs.stat(path.join(source, "a.event")));
  finish();
  await running;
});
