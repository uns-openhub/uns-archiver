import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import { once } from "node:events";
import fs from "node:fs/promises";
import { createServer } from "node:http";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { setTimeout as wait } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import {
  readRetainedStoreManifest, writeRetainedStoreBinding, writeRetainedStoreJson, type StoreBinding,
} from "@uns-kit/core/runtime/retained-store.js";
import { ReplayControl } from "../src/replay-control.js";

const sdkModule: string = "@uns-kit/core/runtime/queued-handover.js";
const sdk = await import(sdkModule).catch(() => null);
const freshStoreSdk = await import("@uns-kit/core/runtime/retained-store.js") as {
  prepareEmptyRetainedStore?: (input: {
    instanceDirectory: string; retainedRoot: string; binding: StoreBinding;
    manifest: ReturnType<typeof readRetainedStoreManifest>;
  }) => void;
};
const endpoint = process.env.ARCHIVER_TEST_QUESTDB;
const mqttEndpoint = process.env.ARCHIVER_TEST_MQTT;
const repo = fileURLToPath(new URL("..", import.meta.url));
const mqtt = createRequire(import.meta.resolve("@uns-kit/core"))("mqtt");

test("built Archiver uses real MQTT and QuestDB during queued handover", {
  skip: !endpoint || !mqttEndpoint || !sdk || !freshStoreSdk.prepareEmptyRetainedStore,
  timeout: 120_000, concurrency: 3,
}, async t => {
  const cases = [
    { fault: "none", name: "cutover retains independent configs and recovers a frozen source" },
    { fault: "capture", name: "capture failure after source exit blocks target release" },
    { fault: "adoption", name: "adoption pause persistence failure blocks release and normal restart" },
  ] as const;
  await Promise.all(cases.map(({ fault, name }) => t.test(name, async () => {
    assert.equal(new URL(endpoint!).hostname, "127.0.0.1");
    assert.equal(new URL(mqttEndpoint!).hostname, "127.0.0.1");
    const id = randomUUID().slice(0, 8);
    const prefix = `test_gap_${id}`;
    const topicRoot = `canary/archiver-gap/${id}`;
    const changed = topicRoot + "/changed";
    const sibling = topicRoot + "/sibling";
    const oldTable = prefix + "_old_changed_data";
    const newTable = prefix + "_new_changed_data";
    const siblingTable = prefix + "_sibling_unchanged_data";
    const root = await fs.realpath(await fs.mkdtemp(path.join(tmpdir(), "archiver-qos0-gap-")));
    const retained = path.join(root, "retained");
    await fs.mkdir(retained, { mode: 0o700 });
    const processes: ReturnType<typeof spawn>[] = [];
    const logs = new Map<ReturnType<typeof spawn>, string>();
    let sourceWrite = false;
    const serviceToken = randomUUID();
    const discovery = createServer((req, res) => {
      req.resume();
      res.setHeader("content-type", "application/json");
      if (req.url !== "/graphql" || req.headers.authorization !== `Bearer ${serviceToken}`) {
        res.writeHead(403).end("{}");
        return;
      }
      res.end(JSON.stringify({ data: { GetUnsNodes: [changed, sibling].map((fullTopic, index) => ({
        id: index + 1, type: "Attribute", fullTopic, attribute: "reading", unsNode: "reading",
        asset: "canary", objectType: "measurement", objectId: "test", attributeNeedsPersistance: true,
      })) } }));
    });
    const sourceGateway = createServer(async (req, res) => {
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(Buffer.from(chunk));
      if (!sourceWrite && req.method === "POST" && req.url?.startsWith("/write")) {
        res.writeHead(503).end("Local fixture holds source ILP");
        return;
      }
      try {
        const response = await fetch(new URL(req.url!, endpoint), {
          method: req.method,
          headers: { "content-type": req.headers["content-type"] ?? "text/plain" },
          ...(req.method === "POST" ? { body: Buffer.concat(chunks) } : {}),
        });
        res.writeHead(response.status, { "content-type": response.headers.get("content-type") ?? "text/plain" })
          .end(Buffer.from(await response.arrayBuffer()));
      } catch { res.writeHead(503).end(); }
    });
    const listen = async (server: typeof discovery) => {
      await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
      return (server.address() as { port: number }).port;
    };
    const discoveryPort = await listen(discovery);
    const sourceGatewayPort = await listen(sourceGateway);
    const client = await mqtt.connectAsync(mqttEndpoint!, { clean: true });
    const template = JSON.parse(await fs.readFile(path.join(repo, "config-development-host.json"), "utf8"));
    await fs.writeFile(path.join(root, "service-token"), serviceToken, { mode: 0o600 });
    const manifest = readRetainedStoreManifest(repo);
    const pkg = JSON.parse(await fs.readFile(path.join(repo, "package.json"), "utf8"));
    const start = (instance: string, launchId: string, env: Record<string, string> = {}) => {
      const child = spawn(process.execPath, [
        path.join(repo, "tests/fixtures/archiver-process-bootstrap.mjs"), path.join(repo, "dist/index.js"),
      ], { cwd: instance, env: {
        PATH: process.env.PATH, NODE_ENV: "test", UNS_API_JWT_SECRET: randomUUID(), UNS_ARCHIVER_TRACE: "1",
        UNS_SERVICE_TOKEN_FILE: path.join(root, "service-token"), RTT_INSTANCE_ID: path.basename(instance),
        UNS_RETAINED_STORE_ROOT: retained, UNS_HANDOVER_LAUNCH_ID: launchId, UNS_HANDOVER_PM2_IPC: "1",
        ...env,
      }, stdio: ["ignore", "pipe", "pipe", "ipc"] });
      processes.push(child);
      for (const stream of [child.stdout!, child.stderr!])
        stream.on("data", bytes => logs.set(child, ((logs.get(child) ?? "") + String(bytes)).slice(-24000)));
      return child;
    };
    type Child = ReturnType<typeof start>;
    const run = async <T>(label: string, poll: () => Promise<T | false> | T | false, timeout = 20000): Promise<T> => {
      const deadline = Date.now() + timeout;
      let reason = "";
      while (Date.now() < deadline) {
        try { const value = await poll(); if (value) return value; }
        catch (error) { reason = error instanceof Error ? error.message : ""; }
        await wait(30);
      }
      throw Error(`${label} timed out ${reason}\n${[...logs.values()].map(log => log.slice(-4000)).join("\n")}`);
    };
    const exchange = (child: Child, launchId: string, action: string, fields: Record<string, unknown> = {}) =>
      new Promise<any>((resolve, reject) => {
        if (!child.connected) { reject(Error("Archiver process exited")); return; }
        const requestId = randomUUID();
        const timeout = setTimeout(() => { child.off("message", onMessage); reject(Error("Archiver status unanswered")); }, 2000);
        const onMessage = (message: any) => {
          if (message?.topic !== sdk!.QUEUED_HANDOVER_RESPONSE_TOPIC || message?.data?.requestId !== requestId) return;
          clearTimeout(timeout); child.off("message", onMessage);
          resolve(sdk!.queuedHandoverResponseSchema.parse(message.data));
        };
        child.on("message", onMessage);
        child.send({ type: "process:msg", topic: sdk!.QUEUED_HANDOVER_TOPIC, data: {
          type: sdk!.QUEUED_HANDOVER_TOPIC, protocol: 1, requestId, launchId, action, ...fields,
        } });
      });
    const status = async (child: Child, launchId: string) => {
      const response = await exchange(child, launchId, "status");
      return response.ok ? response.status : false;
    };
    const install = async (role: "source" | "target", instanceId: string = role) => {
      const instance = path.join(root, instanceId);
      const storeId = randomUUID();
      const store = path.join(retained, storeId);
      await fs.mkdir(instance, { mode: 0o700 });
      const configuration = {
        ...template,
        uns: { graphql: `http://127.0.0.1:${discoveryPort}/graphql`, rest: `http://127.0.0.1:${discoveryPort}/api`,
          processName: `archiver-gap-${instanceId}-${id}`, instanceMode: "force", handover: false, env: "dev" },
        infra: { host: "127.0.0.1", port: Number(new URL(mqttEndpoint!).port) },
        archiver: { ingestQueueMaxEvents: 64, ingestConcurrency: 4, storedReplayIntervalMs: 500,
          targetJournal: { maxBytes: 16 * 1024 ** 2, reserveBytes: 1024 ** 2 } },
        questdb: {
          configurationString: `http::addr=127.0.0.1:${role === "source" ? sourceGatewayPort : Number(new URL(endpoint!).port)};retry_timeout=250;request_timeout=750;`,
          batch: { maxRows: 16, flushIntervalMs: 20 },
          dataStorage: [
            {
              topic: changed, tablePrefix: prefix + (role === "source" ? "_old" : "_new"), ingestMode: "append",
              ...(role === "source" ? { dataGroups: [{ name: "changed", ingestMode: "dedup" }] } : {}),
            },
            { topic: sibling, tablePrefix: prefix + "_sibling", ingestMode: "append" },
          ],
        },
      };
      await fs.writeFile(path.join(instance, "config.json"), JSON.stringify(configuration), { mode: 0o600 });
      await fs.writeFile(path.join(instance, "package.json"), JSON.stringify(pkg), { mode: 0o600 });
      const binding = { protocol: 1 as const, storeId, generation: randomUUID(), operationId: randomUUID(),
        directory: store, format: manifest.format, configurationDigest: "a".repeat(64),
        owner: { rttNode: pkg.name, version: pkg.version, instanceId }, maintenance: true as const };
      if (role === "target") {
        freshStoreSdk.prepareEmptyRetainedStore!({ instanceDirectory: instance, retainedRoot: retained, binding, manifest });
      } else {
        await fs.mkdir(path.join(store, "events"), { recursive: true, mode: 0o700 });
        writeRetainedStoreBinding(instance, binding);
        writeRetainedStoreJson(path.join(store, "store.json"), binding);
        writeRetainedStoreJson(path.join(store, "store-resources.json"), manifest);
        const replay = new ReplayControl(path.join(store, "replay"), () => 0);
        replay.ensureMaintenancePause(binding.operationId);
        replay.command({ action: "resume", requestId: randomUUID(), expectedRevision: 1, reason: "Previously commissioned source" }, "local-test");
      }
      return { instance, store, adoption: binding.operationId, identity: sdk!.queuedHandoverRetainedStoreIdentity(binding) };
    };
    const publish = (topic: string, group: string, value: number) => client.publishAsync(topic, JSON.stringify({
      version: "2.0.0", message: { data: { dataGroup: group, value, time: new Date(1793650000000 + value * 1000).toISOString(), uom: "kWh" } },
    }), { qos: 0 });
    const read = async (table: string) => {
      const response = await fetch(new URL(`/exec?query=${encodeURIComponent(`select numberValue from ${table} order by numberValue`)}`, endpoint));
      const json = await response.json() as { dataset?: [number][]; error?: string };
      if (response.ok) return (json.dataset ?? []).map(row => row[0]);
      assert.match(json.error ?? "", /not found|does not exist/i);
      return [];
    };
    const usesDedup = async (table: string) => {
      const sql = `SELECT dedup FROM tables() WHERE table_name = '${table}'`;
      const response = await fetch(new URL(`/exec?query=${encodeURIComponent(sql)}`, endpoint));
      assert.ok(response.ok);
      const json = await response.json() as { dataset: [boolean][] };
      assert.equal(json.dataset.length, 1);
      return json.dataset[0][0];
    };

    try {
      const source = await install("source");
      const target = await install("target");
      const targetReplayFile = path.join(target.store, "replay");
      await assert.rejects(fs.stat(targetReplayFile), { code: "ENOENT" });
      const sourceLaunchId = randomUUID();
      const targetLaunchId = randomUUID();
      const binding = sdk.queuedHandoverBindingSchema.parse({
        protocol: 1, operationId: randomUUID(), reviewedChecksum: "b".repeat(64),
        sourceLaunchId, targetLaunchId, sourceQueueIdentity: source.identity, targetQueueIdentity: target.identity,
        expiresAt: Date.now() + 90_000, acceptedInputLoss: true,
      });
      if (fault === "none") {
        const compatibility = await install("source", "durable-session");
        const file = path.join(compatibility.instance, "config.json");
        const saved = JSON.parse(await fs.readFile(file, "utf8"));
        saved.infra = { ...saved.infra, clean: false, clientId: `archiver-session-${id}`,
          properties: { sessionExpiryInterval: 30 } };
        await fs.writeFile(file, JSON.stringify(saved), { mode: 0o600 });
        const compatibilityLaunchId = randomUUID();
        const ordinary = start(compatibility.instance, compatibilityLaunchId);
        await run("normal persistent MQTT session is observation only", async () => {
          const view = await status(ordinary, compatibilityLaunchId);
          return view?.phase === "ready" && view.inputBoundary === "none" && view.reason === "input-boundary-unproven";
        });
        const ordinaryExit = once(ordinary, "exit");
        ordinary.kill("SIGTERM");
        assert.deepEqual(await ordinaryExit, [0, null]);
        const incompatibleLaunchId = randomUUID();
        const incompatibleBinding = { ...binding, operationId: randomUUID(), targetLaunchId: incompatibleLaunchId,
          targetQueueIdentity: compatibility.identity };
        const incompatible = start(compatibility.instance, incompatibleLaunchId, {
          UNS_QUEUED_HANDOVER_ROLE: "target", UNS_QUEUED_HANDOVER_BINDING: JSON.stringify(incompatibleBinding),
        });
        assert.deepEqual(await once(incompatible, "exit"), [1, null]);
        assert.match(logs.get(incompatible) ?? "", /clean sessions without expiry/);
        assert.doesNotMatch(logs.get(incompatible) ?? "", /Connected to MQTT broker/);
      }
      const sourceProcess = start(source.instance, sourceLaunchId);
      const targetProcess = start(target.instance, targetLaunchId, {
        UNS_QUEUED_HANDOVER_ROLE: "target", UNS_QUEUED_HANDOVER_BINDING: JSON.stringify(binding),
      });
      await run("held target empty", async () => {
        const view = await status(targetProcess, targetLaunchId);
        return view?.phase === "held" && !view.reason;
      });
      assert.equal(new ReplayControl(targetReplayFile, () => 0).hasInitialMaintenancePause(target.adoption), true);
      await run("source broker subscription acknowledged", async () => {
        const view = await status(sourceProcess, sourceLaunchId);
        return view?.phase === "ready" && !view.reason &&
          (logs.get(sourceProcess)?.includes("No active message received within timeout.") ?? false);
      }, 40_000);
      await publish(changed, "changed", 10);
      await publish(sibling, "unchanged", 11);
      const oldWork = await run("old records durably queued", async () => {
        const view = await status(sourceProcess, sourceLaunchId);
        const lane = view?.queues?.find((item: any) => item.id === "target-journal");
        if (lane?.pending >= 2 && lane.inFlight === 0) return lane.pending;
        throw Error(JSON.stringify(view?.queues?.map(({ id, pending, inFlight }: any) => ({ id, pending, inFlight }))));
      });
      const freeze = await exchange(sourceProcess, sourceLaunchId, "freeze-source", { binding });
      assert.equal(freeze.ok, true, JSON.stringify(freeze));
      const frozen = freeze.status.evidence;
      assert.equal(frozen.phase, "source-frozen");
      await publish(changed, "changed", 99);
      // The publisher's same-connection SUBACK witnesses a broker round trip
      // after the gap marker and before the target receives its open action.
      await client.subscribeAsync(topicRoot + "/gap-witness", { qos: 0 });
      const activate = await exchange(targetProcess, targetLaunchId, "activate-target", { binding, previous: frozen });
      assert.equal(activate.ok, true, JSON.stringify(activate));
      const capturing = activate.status.evidence;
      assert.equal(capturing.phase, "target-capturing");
      await publish(changed, "changed", 20);
      await publish(sibling, "unchanged", 21);
      await run("target capture fsynced", async () =>
        (await fs.readdir(path.join(target.store, "events"))).filter(file => file.endsWith(".event")).length === 2);
      assert.deepEqual(await read(newTable), []);
      assert.deepEqual(await read(siblingTable), []);
      assert.equal(new ReplayControl(targetReplayFile, () => 0).hasInitialMaintenancePause(target.adoption), true);
      const sourceStillPending = await status(sourceProcess, sourceLaunchId);
      assert.ok(sourceStillPending.queues.some((lane: any) => lane.id === "target-journal" && lane.pending >= 2));

      const death = once(sourceProcess, "exit");
      sourceProcess.kill("SIGKILL");
      await death;
      const recoveryLaunchId = randomUUID();
      const recovered = start(source.instance, recoveryLaunchId, {
        UNS_QUEUED_HANDOVER_RECOVERY: JSON.stringify({
          protocol: 1, launchId: recoveryLaunchId, operationId: binding.operationId,
          sourceFrozenDigest: sdk.queuedHandoverEvidenceDigest(frozen),
        }),
      });
      await run("original source reopened frozen", async () => (await status(recovered, recoveryLaunchId))?.phase === "source-frozen");
      sourceWrite = true;
      const drained = await run("old queue drained", async () => {
        const result = await exchange(recovered, recoveryLaunchId, "drain-source", { binding, previous: capturing });
        const view = result.ok ? result.status : await status(recovered, recoveryLaunchId);
        return view?.phase === "source-drained" && view.evidence;
      });
      assert.deepEqual(await read(newTable), []);
      const retired = once(recovered, "exit");
      recovered.kill("SIGTERM");
      assert.deepEqual(await retired, [0, null]);
      if (fault === "capture") {
        const spool = path.join(target.store, "events");
        const holding = spool + ".fixture";
        await fs.rename(spool, holding);
        await fs.writeFile(spool, "", { mode: 0o600 });
        try {
          await publish(changed, "changed", 30);
          await run("post-capture durability failure", () =>
            logs.get(targetProcess)?.includes("Event file persistence failed; disk acceptance is not confirmed.") ?? false);
        } finally {
          await fs.unlink(spool);
          await fs.rename(holding, spool);
        }
        const rejected = await exchange(targetProcess, targetLaunchId, "release-target", { binding, previous: drained });
        assert.equal(rejected.ok, false, JSON.stringify(rejected));
        const blocked = await status(targetProcess, targetLaunchId);
        assert.ok(blocked.reason);
        assert.notEqual(blocked.phase, "target-released");
        assert.deepEqual(await read(newTable), []);
        assert.deepEqual(await read(siblingTable), [11]);
        assert.equal(new ReplayControl(targetReplayFile, () => 0).hasInitialMaintenancePause(target.adoption), true);
        console.log(JSON.stringify({
          verification: "actual-archiver-qos0-target-durability-failure",
          sourceExitedBeforeFailure: true, restoredSpoolStillBlocked: true,
          targetReleaseRejected: true, targetWrites: 0, reason: blocked.reason,
        }));
        return;
      }
      await run("capturing target ready after source exit", async () => {
        const view = await status(targetProcess, targetLaunchId);
        return view?.phase === "target-capturing" && !view.reason;
      });
      if (fault === "adoption") {
        const preserved = targetReplayFile + ".fixture";
        await fs.rename(targetReplayFile, preserved);
        await fs.mkdir(targetReplayFile, { mode: 0o700 });
        try {
          const rejected = await exchange(targetProcess, targetLaunchId, "release-target", { binding, previous: drained });
          assert.equal(rejected.ok, false, JSON.stringify(rejected));
        } finally {
          await fs.rmdir(targetReplayFile);
          await fs.rename(preserved, targetReplayFile);
        }
        const blocked = await status(targetProcess, targetLaunchId);
        assert.equal(blocked.phase, "blocked");
        assert.equal(blocked.reason, "outcome-unknown");
        assert.equal(new ReplayControl(targetReplayFile, () => 0).hasInitialMaintenancePause(target.adoption), true);
        assert.deepEqual(await read(newTable), []);
        assert.deepEqual(await read(siblingTable), [11]);
        const targetExit = once(targetProcess, "exit");
        targetProcess.kill("SIGTERM");
        assert.deepEqual(await targetExit, [0, null]);
        const unauthorized = start(target.instance, randomUUID());
        assert.deepEqual(await once(unauthorized, "exit"), [1, null]);
        assert.match(logs.get(unauthorized) ?? "", /Previous queued handover still fences/);
        assert.doesNotMatch(logs.get(unauthorized) ?? "", /Connected to MQTT broker/);
        console.log(JSON.stringify({
          verification: "actual-archiver-qos0-adoption-pause-persistence-failure",
          targetReleaseRejected: true, targetWrites: 0, reason: blocked.reason,
          ordinaryRestartRejectedBeforeConnect: true,
        }));
        return;
      }
      const release = await exchange(targetProcess, targetLaunchId, "release-target", { binding, previous: drained });
      assert.equal(release.ok, true, JSON.stringify(release));
      assert.equal(release.status.evidence.phase, "target-released");
      await run("all QuestDB rows", async () => (await read(newTable)).length === 1 && (await read(siblingTable)).length === 2);
      const old = await read(oldTable);
      const next = await read(newTable);
      const unchanged = await read(siblingTable);
      assert.deepEqual(old, [10]); assert.deepEqual(next, [20]); assert.deepEqual(unchanged, [11, 21]);
      assert.equal(await usesDedup(oldTable), true);
      assert.equal(await usesDedup(newTable), false);
      assert.equal(await usesDedup(siblingTable), false);
      assert.equal(new ReplayControl(targetReplayFile, () => 0).paused, false);
      console.log(JSON.stringify({
        verification: "actual-archiver-qos0-configuration-handover",
        changedGroup: "changed", unchangedGroup: "unchanged",
        sourceGroupOverride: "dedup", targetGroupOverrideRemoved: true, targetFallback: "append",
        sourceTable: oldTable, targetTable: newTable, unchangedSiblingTable: siblingTable,
        sourceRows: old, targetRows: next, unchangedSiblingRows: unchanged,
        acceptedInputLoss: true, syntheticMarkerDuringGap: 99, syntheticMarkerDelivered: false,
        measuredGapMs: capturing.openedAt - frozen.closedAt, oldWorkWhenTargetCapturing: oldWork,
        realPublisherLossCount: null, targetWritesBeforeRelease: 0,
        killedAndRecoveredSource: true, sourceExitedBeforeTargetRelease: true,
        targetAutomaticAdoptionPauseLiftedAfterRelease: true,
        ordinaryPersistentMqttAllowed: true, incompatibleHeldMqttRejectedBeforeConnect: true,
      }));
    } finally {
      for (const child of processes) {
        if (child.exitCode !== null || child.signalCode !== null) continue;
        const done = once(child, "exit");
        child.kill("SIGTERM");
        const result = await Promise.race([done.then(() => true), wait(3000).then(() => false)]);
        if (!result) { child.kill("SIGKILL"); await done; }
      }
      await client.endAsync(true);
      for (const server of [discovery, sourceGateway])
        await new Promise<void>(resolve => { server.close(() => resolve()); server.closeAllConnections(); });
      await fs.rm(root, { recursive: true, force: true });
    }
  })));
});
