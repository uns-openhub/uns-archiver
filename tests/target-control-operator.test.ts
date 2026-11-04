import assert from "node:assert/strict";
import test from "node:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { createServer } from "node:http";
import { constants, generateKeyPairSync, randomUUID, sign } from "node:crypto";
import UnsApiProxy from "@uns-kit/api/uns-api-proxy.js";
import {
  OPERATOR_API_EXTENSION,
  OPERATOR_AUTH_SCHEME,
  OPERATOR_DELEGATION_CLAIM,
  operatorDigest,
} from "@uns-kit/core/runtime/operator-api.js";
import { TargetJournal, type JournalEntry } from "../src/target-journal.js";
import {
  targetControlApi,
  targetControlOperatorOperations,
  targetStatusOperatorOperations,
} from "../src/target-control-api.js";

test("scoped SDK operator authority recovers JWKS, preserves paused work during denied reads and resumes the durable journal", async () => {
  const root = mkdtempSync(path.join(tmpdir(), "archiver-operator-"));
  const key = generateKeyPairSync("rsa", { modulusLength: 2048 });
  const kid = "synthetic-operator";
  const publicJwk = {
    ...key.publicKey.export({ format: "jwk" }),
    kid,
    alg: "PS256",
    use: "sig",
  };
  const principal = randomUUID();
  let active = true,
    unavailable = false,
    databaseDown = false;
  let jwksReads = 0;
  const rows: JournalEntry<{ value: number }>[] = [];
  const config = {
    directory: path.join(root, "journal"),
    policyFile: path.join(root, "policy.json"),
    reserveBytes: 0,
    retryMs: 5,
    write: async (entry: JournalEntry<{ value: number }>) => {
      if (databaseDown) throw Error("synthetic database outage");
      rows.push(entry);
    },
  };
  let journal = new TargetJournal(config);
  let api = targetControlApi(journal, () => true);
  const owner = {
    controllerName: "local-archiver",
    rttNode: "any-archive-worker",
    version: "5.2.29",
    instanceId: "synthetic",
    launchId: randomUUID(),
  };
  const env = {
    RTT_NODE: owner.rttNode,
    RTT_INSTANCE_ID: owner.instanceId,
    UNS_ROUTE_CONTROLLER_NAME: owner.controllerName,
    UNS_ROUTE_LAUNCH_ID: owner.launchId,
    version: owner.version,
  };
  const previous = Object.fromEntries(
    Object.keys(env).map((key) => [key, process.env[key]]),
  );
  Object.assign(process.env, env);
  const authority = createServer((req, res) => {
    res.setHeader("content-type", "application/json");
    if (req.url === "/operator-jwks") {
      jwksReads++;
      if (jwksReads === 1) {
        res.writeHead(503).end("{}");
        return;
      }
      res.end(JSON.stringify({ keys: [publicJwk] }));
      return;
    }
    res.statusCode = unavailable ? 503 : active ? 200 : 403;
    res.end(
      JSON.stringify({ protocolVersion: 1, active: !unavailable && active }),
    );
  });
  await new Promise<void>((resolve) =>
    authority.listen(0, "127.0.0.1", resolve),
  );
  const authUrl = `http://127.0.0.1:${(authority.address() as { port: number }).port}`;
  const proxy: any = new UnsApiProxy("archiver-operator", "targets", {
    jwtSecret: "unused-native-fixture-secret",
    operatorJwksUrl: authUrl + "/operator-jwks",
    operatorAuthorizationStatusUrl: authUrl + "/status",
  });
  const token = (operation: any, payload: unknown) => {
    const now = Math.floor(Date.now() / 1000);
    const header = Buffer.from(
      JSON.stringify({ alg: "PS256", typ: "uns-operator-api+jwt", kid }),
    ).toString("base64url");
    const claims = Buffer.from(
      JSON.stringify({
        sub: principal,
        role: "operator",
        serviceToken: true,
        identityType: "machine",
        workloadIdentityType: "CONTROLLER_MCP",
        machineIdentityId: principal,
        tokenVersion: 1,
        aud: "uns-operator-api",
        iat: now,
        exp: now + 30,
        jti: randomUUID(),
        accessRules: [operation.path],
        [OPERATOR_DELEGATION_CLAIM]: {
          protocol: 1,
          target: owner,
          operationId: operation.id,
          operationDigest: operatorDigest(operation),
          requestDigest: operatorDigest(payload),
          tool:
            operation.kind === "command"
              ? "controller_execute_runtime_operator_command"
              : "controller_read_runtime_operator_operation",
        },
      }),
    ).toString("base64url");
    const input = header + "." + claims;
    return (
      input +
      "." +
      sign("sha256", Buffer.from(input), {
        key: key.privateKey,
        padding: constants.RSA_PKCS1_PSS_PADDING,
        saltLength: 32,
      }).toString("base64url")
    );
  };
  try {
    proxy.event.on("apiGetEvent", (event: any) => api.get(event));
    proxy.event.on("apiPostEvent", (event: any) => void api.post(event));
    await proxy.get("canary", "archiver", "service", "fixture", "targets", {
      operatorOperations: targetStatusOperatorOperations,
    });
    await proxy.post(
      "canary",
      "archiver",
      "service",
      "fixture",
      "target-control",
      { operatorOperations: targetControlOperatorOperations },
    );
    while (!proxy.app.server.listening)
      await new Promise((resolve) => setTimeout(resolve, 5));
    const apiPath = "/api/canary/archiver/service/fixture";
    const definitions = [
      ...proxy.app.swaggerSpec.paths[apiPath + "/targets"].get[
        OPERATOR_API_EXTENSION
      ],
      ...proxy.app.swaggerSpec.paths[apiPath + "/target-control"].post[
        OPERATOR_API_EXTENSION
      ],
    ];
    const call = (id: string, payload: unknown) => {
      const operation = definitions.find((o: any) => o.id === id);
      assert.ok(operation);
      return fetch(
        `http://127.0.0.1:${proxy.app.server.address().port}${apiPath}/${id === "targets.status" ? "targets" : "target-control"}`,
        {
          method: operation.method,
          headers: {
            authorization:
              OPERATOR_AUTH_SCHEME + " " + token(operation, payload),
            "content-type": "application/json",
          },
          ...(operation.method === "POST"
            ? { body: JSON.stringify(payload) }
            : {}),
        },
      );
    };
    const command = {
      action: "pause",
      requestId: randomUUID(),
      expectedRevision: 0,
      reason: "Synthetic maintenance",
      selector: { tables: ["test_paused_data"] },
    };
    assert.equal(
      (
        await call("targets.preview", {
          action: "preview",
          selector: command.selector,
        })
      ).status,
      200,
    );
    assert.equal(jwksReads, 2);
    assert.equal(journal.snapshot().policy.revision, 0);
    // Discard one successful reply, then repeat the exact application request ID.
    assert.equal((await call("targets.pause", command)).status, 200);
    assert.equal((await call("targets.pause", command)).status, 200);
    assert.equal(journal.snapshot().policy.revision, 1);
    const event = (
      table: string,
      value: number,
    ): JournalEntry<{ value: number }> => ({
      id: randomUUID(),
      table,
      topic: "canary/archiver/value",
      payload: { value },
    });
    await journal.submit([
      event("test_paused_data", 1),
      event("test_active_data", 2),
    ]);
    assert.deepEqual(
      rows.map((row) => row.payload.value),
      [2],
    );
    databaseDown = true;
    await journal.submit([
      event("test_paused_data", 3),
      event("test_active_data", 4),
    ]);
    const status = await call("targets.status", {});
    assert.equal(status.status, 200);
    assert.equal(((await status.json()) as any).policy.revision, 1);
    await journal.close();
    journal = new TargetJournal(config);
    api = targetControlApi(journal, () => true);
    assert.equal(journal.snapshot().policy.revision, 1);
    assert.equal(
      journal
        .snapshot()
        .targets.reduce((n, target) => n + Number(target.queued), 0),
      3,
    );
    active = false;
    assert.equal((await call("targets.status", {})).status, 403);
    active = true;
    unavailable = true;
    assert.equal((await call("targets.status", {})).status, 503);
    unavailable = false;
    const recovered = await call("targets.status", {});
    assert.equal(recovered.status, 200);
    const unchanged = (await recovered.json()) as any;
    assert.equal(unchanged.policy.revision, 1);
    assert.equal(unchanged.boundary.phase, "paused");
    assert.equal(
      unchanged.targets.reduce(
        (n: number, target: any) => n + Number(target.queued),
        0,
      ),
      3,
    );
    assert.equal((await call("targets.pause", command)).status, 200);
    const resume = {
      action: "resume",
      requestId: randomUUID(),
      expectedRevision: 1,
      reason: "Maintenance finished",
      pauseId: command.requestId,
    };
    active = false;
    assert.equal((await call("targets.resume", resume)).status, 403);
    active = true;
    unavailable = true;
    assert.equal((await call("targets.resume", resume)).status, 503);
    unavailable = false;
    databaseDown = false;
    journal.start();
    assert.equal((await call("targets.resume", resume)).status, 200);
    const deadline = Date.now() + 3000;
    while (rows.length < 4 && Date.now() < deadline)
      await new Promise((resolve) => setTimeout(resolve, 10));
    assert.deepEqual(rows.map((row) => row.payload.value).sort(), [1, 2, 3, 4]);
    assert.equal(new Set(rows.map((row) => row.id)).size, 4);
    assert.equal(
      journal
        .snapshot()
        .targets.reduce((n, target) => n + Number(target.queued), 0),
      0,
    );
    assert.equal(journal.snapshot().boundary.phase, "running");
  } finally {
    await proxy.stop();
    await journal.close();
    await new Promise<void>((resolve) => {
      authority.close(() => resolve());
      authority.closeAllConnections();
    });
    for (const [key, value] of Object.entries(previous)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    rmSync(root, { recursive: true, force: true });
  }
});
