import assert from "node:assert/strict";
import test from "node:test";
import { createServer } from "node:http";
import { generateKeyPairSync, randomUUID, sign, verify } from "node:crypto";
import { promises as fs, createWriteStream } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import { spawn, execFile } from "node:child_process";
import { promisify } from "node:util";
import { once } from "node:events";
import { MQTT_COOPERATIVE_STOP_TOPIC } from "@uns-kit/core/runtime/mqtt-drain.js";

const endpoint = process.env.ARCHIVER_TEST_QUESTDB;
const broker = process.env.ARCHIVER_TEST_MQTT;
const databaseContainer = process.env.ARCHIVER_TEST_QDB_CONTAINER;
const run = promisify(execFile);
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const repo = fileURLToPath(new URL("..", import.meta.url));
// Exercise the very MQTT transport used by the installed SDK, without a second client version.
const mqtt = createRequire(import.meta.resolve("@uns-kit/core"))("mqtt");

test(
  "actual Archiver process: MQTT, discovered JWKS API, one-table pause, database outage, lost response and restart",
  {
    skip: !endpoint || !broker,
    timeout: 180000,
  },
  async () => {
    assert.equal(new URL(endpoint!).hostname, "127.0.0.1");
    assert.equal(new URL(broker!).hostname, "127.0.0.1");
    if (databaseContainer)
      assert.match(databaseContainer, /^openhub-archiver-e2e-qdb-\d{8}$/);
    await fs.access(path.join(repo, "dist/index.js"));
    const root = await fs.realpath(
      await fs.mkdtemp(path.join(tmpdir(), "archiver-process-")),
    );
    const evidence = process.env.ARCHIVER_TEST_EVIDENCE;
    if (evidence)
      assert.ok(
        evidence.startsWith("/private/tmp/") || evidence.startsWith("/tmp/"),
      );
    const id = randomUUID().replaceAll("-", "").slice(0, 10);
    const processName = `archiver-test-${id}`;
    const prefix = `test_archive_${id}`;
    const topics = ["paused", "healthy", "other"].map(
      (group) => `canary/archiver-process/${id}/${group}/reading`,
    );
    const tables = [
      `${prefix}_paused_data`,
      `${prefix}_healthy_data`,
      `${prefix}_other_table`,
    ];
    const { privateKey, publicKey } = generateKeyPairSync("rsa", {
      modulusLength: 2048,
    });
    const kid = `local-${id}`;
    const signToken = (
      roles: string[],
      scopes: string[],
      accessRules: string[],
    ) => {
      const header = Buffer.from(
        JSON.stringify({ alg: "RS256", typ: "JWT", kid }),
      ).toString("base64url");
      const payload = Buffer.from(
        JSON.stringify({
          sub: `local-${roles[0]}-${id}`,
          roles,
          scopes,
          accessRules,
          exp: Math.floor(Date.now() / 1000) + 600,
        }),
      ).toString("base64url");
      return `${header}.${payload}.${sign("RSA-SHA256", Buffer.from(`${header}.${payload}`), privateKey).toString("base64url")}`;
    };
    const serviceToken = signToken(
      ["service"],
      ["read:uns"],
      [`canary/archiver-process/${id}/#`],
    );
    const operatorToken = signToken(["operator"], [], ["system/archiver/#"]);
    const readerToken = signToken(["user"], [], ["system/archiver/#"]);
    let discoveryReads = 0;
    // Deliberately scoped discovery/JWKS fixture. This is not a real Controller,
    // a Controller-managed registration test, or a proxy-authorization bypass.
    const discovery = createServer(async (req, res) => {
      res.setHeader("content-type", "application/json");
      if (req.url === "/api/.well-known/jwks.json") {
        res.end(
          JSON.stringify({
            keys: [
              {
                ...publicKey.export({ format: "jwk" }),
                kid,
                alg: "RS256",
                use: "sig",
              },
            ],
          }),
        );
        return;
      }
      if (req.url === "/api/.well-known/kid") {
        res.end(JSON.stringify({ kid }));
        return;
      }
      if (req.url !== "/graphql") {
        res.writeHead(404);
        res.end("{}");
        return;
      }
      const token = req.headers.authorization?.slice(7);
      const parts = token?.split(".") ?? [];
      const valid =
        token === serviceToken &&
        parts.length === 3 &&
        verify(
          "RSA-SHA256",
          Buffer.from(`${parts[0]}.${parts[1]}`),
          publicKey,
          Buffer.from(parts[2], "base64url"),
        );
      if (!valid) {
        res.writeHead(403);
        res.end("{}");
        return;
      }
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(Buffer.from(chunk));
      const body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
      if (!String(body.query).includes("GetUnsNodes")) {
        res.writeHead(400);
        res.end("{}");
        return;
      }
      discoveryReads++;
      res.end(
        JSON.stringify({
          data: {
            GetUnsNodes: topics.map((topic, index) => ({
              id: index + 1,
              type: "Attribute",
              fullTopic: topic,
              unsNode: "reading",
              attribute: "reading",
              asset: "test-equipment",
              objectType: "measurement",
              objectId: "main",
              attributeNeedsPersistance: true,
            })),
          },
        }),
      );
    });
    await new Promise<void>((resolve) =>
      discovery.listen(0, "127.0.0.1", resolve),
    );
    const discoveryUrl = `http://127.0.0.1:${(discovery.address() as any).port}`;
    let transportMode: "up" | "down" | "hold" = "up";
    let releaseHeld: (() => void) | undefined;
    let held = false;
    const gateway = createServer(async (req, res) => {
      try {
        const chunks: Buffer[] = [];
        for await (const chunk of req) chunks.push(Buffer.from(chunk));
        if (transportMode === "down") {
          res.writeHead(503);
          res.end("Local test outage");
          return;
        }
        if (transportMode === "hold" && req.url?.startsWith("/write")) {
          held = true;
          await new Promise<void>((resolve) => {
            releaseHeld = resolve;
          });
        }
        const result = await fetch(`${endpoint}${req.url}`, {
          method: req.method,
          headers: {
            "content-type": req.headers["content-type"] ?? "text/plain",
          },
          ...(req.method !== "GET" ? { body: Buffer.concat(chunks) } : {}),
        });
        res.writeHead(result.status, {
          "content-type": result.headers.get("content-type") ?? "text/plain",
        });
        res.end(Buffer.from(await result.arrayBuffer()));
      } catch {
        res.writeHead(503);
        res.end("Local database unreachable");
      }
    });
    await new Promise<void>((resolve) =>
      gateway.listen(0, "127.0.0.1", resolve),
    );
    const config = JSON.parse(
      await fs.readFile(
        path.join(repo, "config-development-host.json"),
        "utf8",
      ),
    );
    config.uns = {
      graphql: `${discoveryUrl}/graphql`,
      rest: `${discoveryUrl}/api`,
      processName,
      instanceMode: "force",
      handover: false,
      handoverProtocol: "correlated-v2",
      env: "dev",
      jwksWellKnownUrl: `${discoveryUrl}/api/.well-known/jwks.json`,
      kidWellKnownUrl: `${discoveryUrl}/api/.well-known/kid`,
      supervisor: { enabled: false },
    };
    config.infra = { host: "127.0.0.1", port: Number(new URL(broker!).port) };
    config.archiver = {
      ...config.archiver,
      ingestConcurrency: 16,
      ingestQueueMaxEvents: 64,
      targetJournal: { maxBytes: 16 * 1024 * 1024, reserveBytes: 1024 * 1024 },
    };
    config.questdb = {
      configurationString: `http::addr=127.0.0.1:${(gateway.address() as any).port};auto_flush=off;request_timeout=1000;retry_timeout=250;`,
      batch: { maxRows: 16, flushIntervalMs: 20, maxPendingRows: 64 },
      dataStorage: [
        {
          topic: `canary/archiver-process/${id}/#`,
          tablePrefix: prefix,
          ingestMode: "append",
        },
      ],
    };
    await fs.writeFile(path.join(root, "config.json"), JSON.stringify(config), {
      mode: 0o600,
    });
    await fs.writeFile(
      path.join(root, "package.json"),
      await fs.readFile(path.join(repo, "package.json")),
    );
    await fs.writeFile(path.join(root, "service-token"), serviceToken, {
      mode: 0o600,
    });
    const client = await mqtt.connectAsync(broker!, {
      clientId: `test-observer-${id}`,
      clean: true,
    });
    const routes = new Map<string, any>();
    const observations: { metadataPackets: number; inputReady: boolean; topicExclusion?: Record<string, unknown> } = { metadataPackets: 0, inputReady: false };
    client.on("message", (topic: string, bytes: Buffer) => {
      try {
        const value = JSON.parse(bytes.toString());
        if (topic.includes(processName) && Array.isArray(value)) {
          observations.metadataPackets++;
          for (const route of value)
            if (route.apiEndpoint) routes.set(route.attribute, route);
        }
      } catch {}
    });
    await client.subscribeAsync(`uns-infra/uns-archiver/#`);
    let child: ReturnType<typeof spawn> | undefined;
    let generation = 0;
    let launchId = "";
    let passiveChild: ReturnType<typeof spawn> | undefined;
    const logStreams: ReturnType<typeof createWriteStream>[] = [];
    const start = () => {
      generation++;
      launchId = randomUUID();
      routes.clear();
      const stream = createWriteStream(
        path.join(root, `archiver-${generation}.log`),
        { mode: 0o600 },
      );
      logStreams.push(stream);
      child = spawn(process.execPath, [path.join(repo, "dist/index.js")], {
        cwd: root,
        env: {
          PATH: process.env.PATH,
          NODE_ENV: "test",
          UNS_SERVICE_TOKEN_FILE: path.join(root, "service-token"),
          UNS_HANDOVER_LAUNCH_ID: launchId,
        },
        stdio: ["ignore", "pipe", "pipe", "ipc"],
      });
      child.stdout!.pipe(stream, { end: false });
      child.stderr!.pipe(stream, { end: false });
      child.once("close", () => stream.end());
    };
    const waitFor = async <T>(
      label: string,
      fn: () => Promise<T | false> | T | false,
      limit = 45000,
    ): Promise<T> => {
      const until = Date.now() + limit;
      while (Date.now() < until) {
        if (child?.exitCode !== null && child?.exitCode !== undefined)
          throw new Error(
            `Archiver exited while waiting for ${label}; inspect local process log`,
          );
        const value = await fn();
        if (value) return value;
        await sleep(50);
      }
      throw new Error(
        `Timed out: ${label}; inspect local process log at ${root}`,
      );
    };
    const routeUrl = (name: string) => {
      const route = routes.get(name);
      if (!route) return undefined;
      const host = new URL(
        route.apiHost.startsWith("http")
          ? route.apiHost
          : `http://${route.apiHost}`,
      );
      return `http://127.0.0.1:${host.port}${route.apiEndpoint}`;
    };
    const get = async (name: string, token = operatorToken) => {
      const url = routeUrl(name);
      if (!url) return undefined;
      const result = await fetch(url, {
        headers: { authorization: `Bearer ${token}` },
      });
      if (!result.ok) return undefined;
      return result.json() as Promise<any>;
    };
    const ready = async () => {
      await waitFor(
        "registered target and topics routes",
        () =>
          routes.has("targets") &&
          routes.has("target-control") &&
          routes.has("topics"),
      );
      await waitFor("input subscriptions ready", async () => {
        const value = await get("topics");
        return value?.inputReadiness?.phase === "ready";
      });
      observations.inputReady = true;
    };
    const post = (body: unknown, signal?: AbortSignal, token = operatorToken) =>
      fetch(routeUrl("target-control")!, {
        method: "POST",
        headers: {
          authorization: `Bearer ${token}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(body),
        signal,
      });
    const expected = new Map<string, unknown[][]>(
      tables.map((table) => [table, []]),
    );
    const publish = async (index: number, value: number) => {
      const time = new Date(1793700000000 + value * 1000).toISOString();
      const group = ["paused", "healthy", "other"][index];
      const packet = {
        version: "2.0.0",
        message:
          index < 2
            ? { data: { dataGroup: group, time, value, uom: "kWh" } }
            : {
                table: {
                  dataGroup: group,
                  time,
                  columns: { reading: { type: "double", value, uom: "kWh" } },
                },
              },
      };
      await client.publishAsync(topics[index], JSON.stringify(packet), {
        qos: 1,
      });
      expected
        .get(tables[index])!
        .push([time.replace(".000Z", ".000000Z"), value, "kWh", topics[index]]);
    };
    const readRows = async (index: number) => {
      const fields =
        index < 2
          ? "timestamp,numberValue,uom,fullTopic"
          : "timestamp,reading,reading_uom,fullTopic";
      const result = await fetch(
        `${endpoint}/exec?query=${encodeURIComponent(`select ${fields} from ${tables[index]} order by timestamp`)}`,
      );
      const body = (await result.json()) as any;
      return result.ok ? (body.dataset as unknown[][]) : [];
    };
    const stopProcess = async (signal: NodeJS.Signals) => {
      if (!child || child.exitCode !== null || child.signalCode !== null)
        return;
      const stopped = once(child, "exit");
      child.kill(signal);
      let timeout: ReturnType<typeof setTimeout> | undefined;
      const result = await Promise.race([
        stopped,
        new Promise((resolve) => {
          timeout = setTimeout(() => resolve("timeout"), 15000);
        }),
      ]).finally(() => clearTimeout(timeout));
      assert.notEqual(
        result,
        "timeout",
        "Archiver did not stop in its local bound",
      );
      if (signal === "SIGTERM") assert.equal(child.exitCode, 0);
    };
    let databaseStopped = false;
    try {
      await waitFor("local QuestDB", async () => {
        try {
          return (await fetch(`${endpoint}/exec?query=select%201`)).ok;
        } catch {
          return false;
        }
      });
      start();
      await ready();
      // Readiness metadata is now published only by the active SDK owner.
      // Verify a genuinely passive successor instead of assuming a startup race.
      const passiveRoot = path.join(root, "passive");
      await fs.mkdir(passiveRoot, { mode: 0o700 });
      await fs.writeFile(
        path.join(passiveRoot, "config.json"),
        JSON.stringify({
          ...config,
          uns: { ...config.uns, instanceMode: "wait", handover: true },
        }),
        { mode: 0o600 },
      );
      await fs.copyFile(
        path.join(root, "package.json"),
        path.join(passiveRoot, "package.json"),
      );
      const passiveLog = path.join(root, "archiver-passive.log");
      await fs.writeFile(passiveLog, "", { mode: 0o600 });
      const stream = createWriteStream(passiveLog, { mode: 0o600 });
      logStreams.push(stream);
      passiveChild = spawn(
        process.execPath,
        [path.join(repo, "dist/index.js")],
        {
          cwd: passiveRoot,
          env: {
            PATH: process.env.PATH,
            NODE_ENV: "test",
            UNS_SERVICE_TOKEN_FILE: path.join(root, "service-token"),
            UNS_HANDOVER_ID: `passive-test-${id}`,
          },
          stdio: ["ignore", "pipe", "pipe"],
        },
      );
      passiveChild.stdout!.pipe(stream, { end: false });
      passiveChild.stderr!.pipe(stream, { end: false });
      passiveChild.once("close", () => stream.end());
      const passivePort = await waitFor("passive API listening", async () => {
        const text = await fs.readFile(passiveLog, "utf8");
        return (
          [
            ...text.matchAll(/API listening on http:\/\/[^:\s]+:(\d+)\/api/g),
          ].at(-1)?.[1] || false
        );
      });
      const passiveUrl = new URL(routeUrl("target-control")!);
      passiveUrl.port = String(passivePort);
      const premature = await fetch(passiveUrl, {
        method: "POST",
        headers: {
          authorization: `Bearer ${operatorToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          action: "pause",
          requestId: randomUUID(),
          expectedRevision: 0,
          reason: "Passive-owner guard",
          selector: { tables: [tables[0]] },
        }),
      });
      assert.equal(premature.status, 503); // Existing shared SDK ownership gate.
      const passiveExit = once(passiveChild, "exit");
      passiveChild.kill("SIGTERM");
      const [passiveCode] = await passiveExit;
      assert.equal(passiveCode, 0);
      assert.equal((await get("targets")).policy.revision, 0);
      assert.equal((await fetch(routeUrl("targets")!)).status, 401);
      assert.equal(
        (
          await fetch(routeUrl("targets")!, {
            headers: { authorization: `Bearer ${readerToken}` },
          })
        ).status,
        403,
      );
      await publish(0, 1);
      await publish(1, 2);
      await publish(2, 3);
      await waitFor("first actual MQTT rows", async () =>
        (await Promise.all([0, 1, 2].map(readRows))).every(
          (rows) => rows.length === 1,
        ),
      );

      transportMode = "hold";
      await publish(0, 10);
      await waitFor("in-flight ILP boundary", () => held);
      const pause = {
        action: "pause",
        requestId: randomUUID(),
        expectedRevision: 0,
        reason: "Local table maintenance",
        selector: { tables: [tables[0]] },
      };
      const abort = new AbortController();
      const abandoned = post(pause, abort.signal).catch(() => undefined);
      await waitFor(
        "persisted pause before reply",
        async () => (await get("targets"))?.policy.revision === 1,
      );
      abort.abort();
      await abandoned;
      const retry = await post(pause);
      assert.equal(retry.status, 200);
      assert.equal(((await retry.json()) as any).boundary.phase, "draining");
      transportMode = "up";
      releaseHeld?.();
      await waitFor(
        "real flush settled",
        async () => (await get("targets"))?.boundary.phase === "paused",
      );
      await publish(0, 20);
      await publish(0, 20);
      await publish(1, 30);
      await publish(2, 31);
      await waitFor(
        "healthy destinations continue",
        async () =>
          (await readRows(1)).length === 2 && (await readRows(2)).length === 2,
      );
      assert.equal((await readRows(0)).length, 2);

      if (databaseContainer) {
        await run("podman", ["stop", "--time", "5", databaseContainer]);
        databaseStopped = true;
      } else transportMode = "down";
      await publish(0, 40);
      await publish(1, 41);
      await publish(2, 42);
      await waitFor(
        "all targets retained during outage",
        async () =>
          (await get("targets"))?.targets.reduce(
            (sum: number, target: any) => sum + target.queued,
            0,
          ) === 5,
      );
      await stopProcess("SIGKILL");
      start();
      await ready();
      assert.equal((await get("targets")).policy.revision, 1);
      await publish(0, 50);
      await publish(1, 51);
      await publish(2, 52);
      await waitFor(
        "input accepted after restart with database down",
        async () =>
          (await get("targets"))?.targets.reduce(
            (sum: number, target: any) => sum + target.queued,
            0,
          ) === 8,
      );
      if (databaseContainer) {
        await run("podman", ["start", databaseContainer]);
        databaseStopped = false;
      } else transportMode = "up";
      await waitFor("database recovery", async () => {
        try {
          return (await fetch(`${endpoint}/exec?query=select%201`)).ok;
        } catch {
          return false;
        }
      });
      await waitFor("unpaused target replay", async () =>
        (await get("targets"))?.targets
          .filter((target: any) => !target.paused)
          .every((target: any) => target.queued === 0),
      );
      await waitFor(
        "unpaused rows visible",
        async () =>
          (await readRows(1)).length === 4 && (await readRows(2)).length === 4,
      );
      assert.equal((await readRows(0)).length, 2);
      assert.equal(
        (await get("targets")).targets.find((target: any) => target.paused)
          .queued,
        4,
      );
      const resume = {
        action: "resume",
        requestId: randomUUID(),
        pauseId: pause.requestId,
        expectedRevision: 1,
        reason: "Database read-back reviewed",
      };
      assert.equal((await post(resume, undefined, readerToken)).status, 403);
      const response = await post(resume);
      assert.equal(response.status, 200);
      assert.equal(((await response.json()) as any).revision, 2);
      const repeated = await post(resume);
      assert.equal(repeated.status, 200);
      assert.equal(((await repeated.json()) as any).revision, 2);
      await waitFor(
        "resumed rows visible",
        async () => (await readRows(0)).length === 6,
      );
      let actual = await Promise.all([0, 1, 2].map(readRows));
      for (let index = 0; index < tables.length; index++)
        assert.deepEqual(actual[index], expected.get(tables[index]));
      const finalStatus = await get("targets");
      assert.equal(finalStatus.boundary.phase, "running");
      assert.equal(
        finalStatus.targets.reduce((n: number, t: any) => n + t.queued, 0),
        0,
      );
      assert.ok(discoveryReads >= 2);
      await stopProcess("SIGTERM");
      start();
      await ready();
      // Exclude one subtree through the same configuration file used by the
      // installed process. Wait for its normal refresh, without restarting it.
      config.questdb.excludeTopics = [topics[0].split("/").slice(0, -1).join("/") + "/#"];
      await fs.writeFile(path.join(root, "config.json"), JSON.stringify(config), { mode: 0o600 });
      await waitFor("live topic exclusion config refresh", async () => {
        const state = await get("topics");
        return state?.topicAdmission?.excludeTopics?.[0] === config.questdb.excludeTopics[0];
      }, 60000);
      await client.publishAsync(topics[0], JSON.stringify({ version: "2.0.0", message: { data: { dataGroup: "paused",
        time: new Date(1793700000000 + 99000).toISOString(), value: 99, uom: "kWh" } } }), { qos: 1 });
      await publish(1, 66);
      await waitFor("excluded MQTT event counted and unrelated row written", async () => {
        const state = await get("topics"); return state?.topicAdmission?.excludedEvents === 1 && (await readRows(1)).length === 5;
      });
      assert.equal((await readRows(0)).length, 6);
      assert.equal((await get("targets")).targets.reduce((n: number, target: any) => n + target.queued, 0), 0);
      config.questdb.excludeTopics = [];
      await fs.writeFile(path.join(root, "config.json"), JSON.stringify(config), { mode: 0o600 });
      await stopProcess("SIGTERM"); start(); await ready();
      assert.deepEqual((await get("topics")).topicAdmission.excludeTopics, []);
      await publish(0, 77);
      await waitFor("previously excluded topic restored", async () => (await readRows(0)).length === 7);
      actual = await Promise.all([0, 1, 2].map(readRows));
      for (let index = 0; index < tables.length; index++) assert.deepEqual(actual[index], expected.get(tables[index]));
      observations.topicExclusion = { liveReload: true, excludedNewEvents: 1, excludedRowsWritten: 0, healthyRowsWritten: 1, clearedAfterRestart: true };
      const cooperativeExit = once(child!, "exit");
      child!.send({
        type: "process:msg",
        data: { type: MQTT_COOPERATIVE_STOP_TOPIC, protocol: 1, launchId },
      });
      let cooperativeTimer: ReturnType<typeof setTimeout> | undefined;
      const cooperativeResult = await Promise.race([
        cooperativeExit,
        new Promise((resolve) => {
          cooperativeTimer = setTimeout(() => resolve("timeout"), 15000);
        }),
      ]).finally(() => clearTimeout(cooperativeTimer));
      assert.notEqual(
        cooperativeResult,
        "timeout",
        "SDK cooperative Stop must exit naturally without a forced signal",
      );
      assert.equal(child!.exitCode, 0);
      const summary = {
        actualProcess: true,
        controllerDiscovery:
          "authenticated scoped fixture, not Controller install",
        auth: "real SDK JWKS",
        databaseOutage: databaseContainer
          ? "actual local container stop/start"
          : "local HTTP injection",
        processGenerations: generation,
        cooperativeStopExit: 0,
        rowsPerTable: actual.map((rows) => rows.length),
        rowsVerified: actual.flat().length,
        queued: 0,
        observations,
        discoveryReads,
        finalStatus,
        actual,
      };
      if (evidence)
        await fs.writeFile(evidence, JSON.stringify(summary, null, 2), {
          mode: 0o600,
        });
      console.log(
        JSON.stringify({
          verification: "actual-archiver-process",
          rows: actual.flat().length,
          perTable: actual.map((rows) => rows.length),
          generations: generation,
          queued: 0,
          localLogs: root,
        }),
      );
    } finally {
      transportMode = "up";
      releaseHeld?.();
      if (
        passiveChild &&
        passiveChild.exitCode === null &&
        passiveChild.signalCode === null
      ) {
        passiveChild.kill("SIGKILL");
        await once(passiveChild, "exit");
      }
      if (child && child.exitCode === null && child.signalCode === null) {
        child.kill("SIGKILL");
        await once(child, "exit");
      }
      if (databaseStopped && databaseContainer)
        await run("podman", ["start", databaseContainer]);
      await client.endAsync();
      for (const server of [gateway, discovery])
        await new Promise<void>((resolve) => {
          server.close(() => resolve());
          server.closeAllConnections();
        });
      for (const stream of logStreams) stream.end();
      await fs.unlink(path.join(root, "service-token")).catch(() => undefined);
      // Preserve this run's synthetic journal/config/logs for independent review.
      console.log(`Local process evidence: ${root}`);
    }
  },
);
