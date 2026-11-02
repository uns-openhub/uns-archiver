import assert from "node:assert/strict";
import test from "node:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { createHmac, randomBytes, randomUUID } from "node:crypto";
import UnsApiProxy from "@uns-kit/api/uns-api-proxy.js";
import { TargetJournal } from "../src/target-journal.js";
import {
  targetControlApi,
  targetCommandOpenApiSchema,
} from "../src/target-control-api.js";

test("real SDK HTTP authentication: anonymous/forged/reader/path denial and operator lost-response retry", async () => {
  const root = mkdtempSync(path.join(tmpdir(), "archiver-target-http-"));
  const secret = randomBytes(32).toString("hex");
  const journal = new TargetJournal({
    directory: path.join(root, "journal"),
    policyFile: path.join(root, "policy.json"),
    write: async () => {},
    reserveBytes: 0,
  });
  const proxy = new UnsApiProxy("archiver-test", "target-http", {
    jwtSecret: secret,
  });
  let activeOwner = true;
  const api = targetControlApi(journal, () => activeOwner);
  const sign = (role: string, pathFilter = "#", signingSecret = secret) => {
    const header = Buffer.from(
      JSON.stringify({ alg: "HS256", typ: "JWT" }),
    ).toString("base64url");
    const payload = Buffer.from(
      JSON.stringify({
        sub: "local-operator",
        roles: [role],
        pathFilter,
        exp: Math.floor(Date.now() / 1000) + 60,
      }),
    ).toString("base64url");
    return `${header}.${payload}.${createHmac("sha256", signingSecret).update(`${header}.${payload}`).digest("base64url")}`;
  };
  try {
    proxy.event.on("apiGetEvent", (event) => api.get(event));
    proxy.event.on("apiPostEvent", (event) => {
      void api.post(event);
    });
    await proxy.get("canary", "archiver", "service", "local", "targets");
    await proxy.post(
      "canary",
      "archiver",
      "service",
      "local",
      "target-control",
      { requestBody: { required: true, schema: targetCommandOpenApiSchema } },
    );
    const base = `http://127.0.0.1:${(proxy as any).app.server.address().port}/api/canary/archiver/service/local`;
    const call = (token?: string, body?: unknown) =>
      fetch(`${base}/${body ? "target-control" : "targets"}`, {
        method: body ? "POST" : "GET",
        headers: {
          ...(token ? { authorization: `Bearer ${token}` } : {}),
          "content-type": "application/json",
        },
        ...(body ? { body: JSON.stringify(body) } : {}),
      });
    assert.equal((await call()).status, 401);
    assert.equal((await call(sign("admin", "#", "wrong-secret"))).status, 401);
    assert.equal((await call(sign("admin", "unrelated/#"))).status, 403);
    assert.equal((await call(sign("user"))).status, 403);
    const command = {
      action: "pause",
      requestId: randomUUID(),
      expectedRevision: 0,
      reason: "Local test",
      selector: { tables: ["test_http_data"] },
    };
    assert.equal((await call(sign("user"), command)).status, 403);
    // Discard the first response as a client with a lost answer would do.
    assert.equal((await call(sign("operator"), command)).status, 200);
    const retry = await call(sign("operator"), command);
    assert.equal(retry.status, 200);
    assert.equal(((await retry.json()) as any).revision, 1);
    const status = await call(sign("admin"));
    assert.equal(status.status, 200);
    assert.equal(((await status.json()) as any).policy.rules.length, 1);
    const resume = {
      action: "resume",
      requestId: randomUUID(),
      expectedRevision: 1,
      pauseId: command.requestId,
      reason: "Reviewed local resume",
    };
    activeOwner = false;
    assert.equal((await call(sign("operator"), resume)).status, 409);
    assert.equal(
      (
        await call(sign("operator"), {
          action: "preview",
          selector: { tables: ["test_http_data"] },
        })
      ).status,
      200,
    );
    assert.equal(journal.snapshot().policy.revision, 1);
    activeOwner = true;
    assert.equal((await call(sign("operator"), resume)).status, 200);
    assert.equal(journal.snapshot().boundary.phase, "running");
  } finally {
    await proxy.stop();
    await journal.close();
    rmSync(root, { recursive: true, force: true });
  }
});
