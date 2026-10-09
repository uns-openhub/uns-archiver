import assert from "node:assert/strict";
import { createServer } from "node:http";
import { promises as fs } from "node:fs";
import path from "node:path";
import { performance, monitorEventLoopDelay } from "node:perf_hooks";
import { randomUUID } from "node:crypto";
import { tmpdir } from "node:os";
import { Sender } from "@questdb/nodejs-client";
import { LegacyImportManager } from "../src/legacy-import.js";
import { writeLegacyEvent } from "../src/legacy-import-writer.js";
import { QuestDBWriter } from "../src/writers/questDbWriter.js";

// Opt-in acceptance against a disposable local QuestDB. No production URL,
// credentials or fixture payload are accepted or emitted by this runner.
const count = Number(process.argv[2] ?? 20_000);
assert.ok(Number.isSafeInteger(count) && count >= 512 && count <= 100_000);
const output = process.argv[3];
const database = "http://127.0.0.1:9000";
const root = await fs.realpath(
  await fs.mkdtemp(path.join(tmpdir(), "openhub-p4-questdb-import-")),
);
const old = path.join(root, "old");
const live = path.join(root, "live");
await fs.mkdir(old);
await fs.mkdir(live);
const prefix = `p4_import_${randomUUID().replaceAll("-", "")}`;
const table = `${prefix}_data`;
const topic = "fixture/retired/equipment/motor/speed";
const epoch = Date.parse("2026-07-19T12:00:00Z");
const evidence: Record<string, unknown> = {
  scope: "real-QuestDB-HTTP-ILP-and-production-importer-modules",
  excludes: [
    "full-runtime-API",
    "MQTT-handover",
    "5.2.17-upgrade",
    "production-scale",
  ],
  eventCount: count,
  table,
};
const sql = async (query: string) => {
  const response = await fetch(
    `${database}/exec?limit=0,100001&query=${encodeURIComponent(query)}`,
    {
      signal: AbortSignal.timeout(5000),
    },
  );
  const body = (await response.json()) as any;
  assert.equal(response.status, 200, body.error);
  return body;
};
let mode: "slow" | "unavailable" | "normal" = "slow";
let writes = 0;
let failedRequests = 0;
let timer: ReturnType<typeof setTimeout> | undefined;
const sockets = new Set<import("node:net").Socket>();
const proxy = createServer(async (req, res) => {
  try {
    const buffers: Buffer[] = [];
    for await (const piece of req) buffers.push(Buffer.from(piece));
    const isWrite = req.method === "POST";
    if (isWrite) writes++;
    if (isWrite && mode === "unavailable") {
      failedRequests++;
      res
        .writeHead(503, { "content-type": "text/plain" })
        .end("local acceptance outage");
      return;
    }
    if (isWrite && mode === "slow")
      await new Promise((resolve) => setTimeout(resolve, 250));
    const response = await fetch(`${database}${req.url}`, {
      method: req.method,
      headers: {
        "content-type": String(req.headers["content-type"] ?? "text/plain"),
      },
      ...(isWrite ? { body: Buffer.concat(buffers) } : {}),
      signal: AbortSignal.timeout(5000),
    });
    res.writeHead(response.status, {
      "content-type": response.headers.get("content-type") ?? "text/plain",
    });
    res.end(Buffer.from(await response.arrayBuffer()));
  } catch {
    if (!res.headersSent) res.writeHead(502);
    res.end("local acceptance upstream failure");
  }
});
proxy.on("connection", (socket) => {
  sockets.add(socket);
  socket.on("close", () => sockets.delete(socket));
});
await new Promise<void>((resolve) => proxy.listen(0, "127.0.0.1", resolve));
const proxyPort = (proxy.address() as import("node:net").AddressInfo).port;
const sender = await Sender.fromConfig(
  `http::addr=127.0.0.1:${proxyPort};auto_flush=off;retry_timeout=1000;request_timeout=2000;`,
);
const writer = new QuestDBWriter(sender, undefined, {
  maxRows: 64,
  maxPendingRows: 256,
  flushIntervalMs: 20,
});
const createManager = () =>
  new LegacyImportManager({
    sources: [{ id: "old", directory: old }],
    liveDirectory: live,
    instanceId: "p4-real-questdb",
    policyDigest: () => "fixture-append-v1",
    settings: {
      batchSize: 64,
      concurrency: 64,
      intervalMs: 100,
      maxFileBytes: 2048,
      maxBatchBytes: 1024 * 1024,
    },
    canWrite: () => true,
    hasLiveHeadroom: () => true,
    write: (event) =>
      writeLegacyEvent(event, {
        findStorage: () => ({ ingestMode: "append" }),
        write: (packet) => writer.writeUnsPacket(packet, prefix, event.topic),
      }),
  });
let manager = createManager();
const loop = monitorEventLoopDelay({ resolution: 10 });
let maximumRss = process.memoryUsage().rss;
const statusTimes: number[] = [];
const command = (action: "start" | "pause" | "resume") =>
  manager.command({
    action,
    sourceId: "old",
    expectedRevision: manager.status()[0].job?.revision ?? 0,
    requestId: randomUUID(),
    confirmSourceClosed: true,
  });
const waitFor = async (predicate: () => boolean, timeoutMs = 5000) => {
  const deadline = performance.now() + timeoutMs;
  while (!predicate()) {
    assert.ok(
      performance.now() < deadline,
      "Timed out waiting for acceptance condition",
    );
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
};
try {
  await sql("select 1");
  // Match the existing production table contract; append mode does not issue
  // DDL. Automatic ILP table creation would designate a `timestamp` column.
  await sql(
    `create table ${table} (time timestamp) timestamp(time) partition by day`,
  );
  evidence.tableProvisioning = "explicit-existing-time-column-contract";
  for (let start = 0; start < count; start += 32) {
    await Promise.all(
      Array.from({ length: Math.min(32, count - start) }, async (_, offset) => {
        const id = start + offset;
        await fs.writeFile(
          path.join(
            old,
            `${id}.event${id % 1000 === 0 ? ".99999999.100.processing" : ""}`,
          ),
          JSON.stringify({
            topic,
            message: JSON.stringify({
              version: "2.0.0",
              message: {
                data: { value: id, time: new Date(epoch + id).toISOString() },
              },
            }),
          }),
        );
      }),
    );
  }
  loop.enable();
  timer = setInterval(() => {
    const start = performance.now();
    JSON.stringify(manager.status());
    statusTimes.push(performance.now() - start);
    maximumRss = Math.max(maximumRss, process.memoryUsage().rss);
  }, 10);
  await command("start");
  const slow = manager.tick();
  await waitFor(() => writes > 0);
  const pauseStarted = performance.now();
  await command("pause");
  evidence.pauseDuringSlowFlushMs = performance.now() - pauseStarted;
  assert.equal(manager.status()[0].job?.state, "paused");
  await slow; // In-flight acknowledged writes may finish after pause.
  const acknowledged = manager.status()[0].job!.written;
  const immediateRows = (await sql(`select count() from ${table}`))
    .dataset[0][0];
  const visibilityStarted = performance.now();
  let visibleRows = immediateRows;
  while (visibleRows !== acknowledged) {
    assert.ok(
      performance.now() - visibilityStarted < 10_000,
      "Acknowledged rows did not become visible",
    );
    await new Promise((resolve) => setTimeout(resolve, 100));
    visibleRows = (await sql(`select count() from ${table}`)).dataset[0][0];
  }
  evidence.slowFlush = {
    acknowledgedFiles: acknowledged,
    immediateRows,
    visibleRows,
    readVisibilityDelayMs: performance.now() - visibilityStarted,
  };
  mode = "unavailable";
  await command("resume");
  const beforeFailure = manager.status()[0].job!.written;
  await new Promise((resolve) => setTimeout(resolve, 110));
  const failureStarted = performance.now();
  await manager.tick();
  await command("pause");
  assert.equal(
    manager.status()[0].job!.written,
    beforeFailure,
    "Failed ILP must not acknowledge source files",
  );
  assert.ok(manager.status()[0].job!.deferred > 0 && failedRequests > 0);
  evidence.outage = {
    durationMs: performance.now() - failureStarted,
    failedRequests,
    deferred: manager.status()[0].job!.deferred,
    acknowledgedFilesUnchanged: true,
  };
  await manager.close();
  manager = createManager();
  await manager.tick();
  assert.equal(
    manager.status()[0].job!.state,
    "paused",
    "Restart must require explicit resume",
  );
  evidence.restartRemainedPaused = true;
  mode = "normal";
  await command("resume");
  const started = performance.now();
  const deadline = started + 360_000;
  while (manager.status()[0].job!.state === "running") {
    assert.ok(
      performance.now() < deadline,
      "Import exceeded bounded acceptance duration",
    );
    await manager.tick();
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  assert.equal(manager.status()[0].job!.state, "completed");
  const data = await sql(`select value,time from ${table}`);
  assert.equal(
    data.count,
    count,
    "Actual QuestDB row count must match the fixture oracle",
  );
  const seen = new Set<number>();
  for (const [id, timestamp] of data.dataset) {
    assert.ok(Number.isInteger(id) && id >= 0 && id < count && !seen.has(id));
    assert.equal(
      Date.parse(timestamp),
      epoch + id,
      "Historical timestamp changed",
    );
    seen.add(id);
  }
  assert.equal(seen.size, count);
  const residual = (await fs.readdir(old)).filter((name) =>
    /\.event(?:\.\d+\.\d+\.processing)?$/.test(name),
  );
  assert.equal(residual.length, 0);
  clearInterval(timer);
  loop.disable();
  statusTimes.sort((a, b) => a - b);
  Object.assign(evidence, {
    result: "PASS",
    elapsedRecoveryImportMs: performance.now() - started,
    verifiedRows: seen.size,
    timestampMismatches: 0,
    duplicateRows: 0,
    residualSourceEvents: 0,
    importer: manager.status()[0].job,
    writer: writer.getBatchDiagnostics(),
    maximumRssBytes: maximumRss,
    eventLoopDelayMaxMs: loop.max / 1e6,
    statusHelperSamples: statusTimes.length,
    statusHelperP99Ms:
      statusTimes[
        Math.min(statusTimes.length - 1, Math.floor(statusTimes.length * 0.99))
      ],
  });
  if (output)
    await fs.writeFile(output, JSON.stringify(evidence, null, 2) + "\n");
  console.log(JSON.stringify(evidence, null, 2));
} finally {
  clearInterval(timer);
  loop.disable();
  await manager.close();
  await writer.close();
  for (const socket of sockets) socket.destroy();
  await new Promise<void>((resolve) => proxy.close(() => resolve()));
  // Drop only this run's randomly named fixture table.
  await sql(`drop table if exists ${table}`);
  await fs.rm(root, { recursive: true, force: true });
}
