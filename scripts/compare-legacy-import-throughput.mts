import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import { randomUUID } from "node:crypto";
import { performance, monitorEventLoopDelay } from "node:perf_hooks";
import { Sender } from "@questdb/nodejs-client";
import { LegacyImportManager } from "../src/legacy-import.js";
import { resolveLegacyImportConcurrency } from "../src/stored-replay-limits.js";
import { writeLegacyEvent } from "../src/legacy-import-writer.js";
import { QuestDBWriter } from "../src/writers/questDbWriter.js";
// Local-only, independently checked benchmark. Fixture creation and SQL verification
// are outside import timing. Recreate the same synthetic records for each profile.
const count = Number(process.argv[2] ?? 4096),
  profile = process.argv[3] ?? "baseline",
  output = process.argv[4];
assert.ok(Number.isSafeInteger(count) && count >= 512 && count <= 100000);
const profiles = {
  baseline: {
    batchSize: 128,
    concurrency: 64,
    liveQueueCapacity: 512,
    intervalMs: 500,
  },
  "bounded-fast": {
    batchSize: 128,
    concurrency: 64,
    liveQueueCapacity: 512,
    intervalMs: 100,
  },
  "larger-ilp": {
    batchSize: 128,
    concurrency: 128,
    liveQueueCapacity: 1024,
    intervalMs: 100,
  },
  "larger-pass": {
    batchSize: 256,
    concurrency: 64,
    liveQueueCapacity: 512,
    intervalMs: 100,
  },
  "larger-both": {
    batchSize: 256,
    concurrency: 128,
    liveQueueCapacity: 1024,
    intervalMs: 100,
  },
  "short-pacing": {
    batchSize: 256,
    concurrency: 128,
    liveQueueCapacity: 1024,
    intervalMs: 10,
  },
};
assert.ok(Object.hasOwn(profiles, profile));
const settings = profiles[profile as keyof typeof profiles];
const timestampFixture = process.argv[5] ?? "compact";
assert.ok(["compact", "reverse-days"].includes(timestampFixture));
const eventTime = (id: number) =>
  epoch +
  (timestampFixture === "reverse-days" ? (count - 1 - id) * 60_000 : id);
const root = await fs.realpath(
  await fs.mkdtemp(path.join(tmpdir(), "openhub-import-compare-")),
);
const source = path.join(root, "old"),
  live = path.join(root, "live");
await fs.mkdir(source);
await fs.mkdir(live);
const prefix = `p4_speed_${randomUUID().replaceAll("-", "")}`,
  table = `${prefix}_data`;
const sql = async (query: string) => {
  const r = await fetch(
    "http://127.0.0.1:9000/exec?limit=0,100001&query=" +
      encodeURIComponent(query),
    { signal: AbortSignal.timeout(10000) },
  );
  const b = (await r.json()) as any;
  assert.equal(r.status, 200, b.error);
  return b;
};
const epoch = Date.parse("2026-07-19T12:00:00Z");
const sender = await Sender.fromConfig(
  "http::addr=127.0.0.1:9000;auto_flush=off;",
);
const writer = new QuestDBWriter(sender, undefined, {
  maxRows: 512,
  maxPendingRows: 2048,
  flushIntervalMs: 1000,
});
const manager = new LegacyImportManager({
  sources: [{ id: "old", directory: source }],
  liveDirectory: live,
  instanceId: "speed-acceptance",
  policyDigest: () => "speed-append",
  settings: {
    batchSize: settings.batchSize,
    concurrency: resolveLegacyImportConcurrency(
      settings.liveQueueCapacity,
      settings.concurrency,
    ),
    intervalMs: settings.intervalMs,
    maxFileBytes: 2048,
    maxBatchBytes: 1024 * 1024,
  },
  canWrite: () => true,
  hasLiveHeadroom: () => true,
  write: (event) =>
    writeLegacyEvent(event, {
      findStorage: () => ({ ingestMode: "append" }),
      write: (packet) =>
        writer.writeUnsPacket(
          packet,
          prefix,
          event.topic,
          undefined,
          undefined,
          undefined,
          profile === "baseline" ? undefined : { maxBatchWaitMs: 25 },
        ),
    }),
});
const loop = monitorEventLoopDelay({ resolution: 10 });
let rss = process.memoryUsage().rss;
let maximumImportRss = 0;
let measuringImport = false;
let statusMax = 0;
let sample: ReturnType<typeof setInterval> | undefined;
try {
  await sql(
    `create table ${table} (time timestamp) timestamp(time) partition by day WAL`,
  );
  for (let start = 0; start < count; start += 32)
    await Promise.all(
      Array.from({ length: Math.min(32, count - start) }, (_, offset) => {
        const id = start + offset;
        return fs.writeFile(
          path.join(source, `${id}.event`),
          JSON.stringify({
            topic: "fixture/retired/motor/speed",
            message: JSON.stringify({
              version: "2.0.0",
              message: {
                data: {
                  value: id,
                  time: new Date(eventTime(id)).toISOString(),
                },
              },
            }),
          }),
        );
      }),
    );
  await manager.command({
    action: "start",
    sourceId: "old",
    expectedRevision: 0,
    requestId: randomUUID(),
    confirmSourceClosed: true,
  });
  loop.enable();
  sample = setInterval(() => {
    const sampledRss = process.memoryUsage().rss;
    rss = Math.max(rss, sampledRss);
    if (measuringImport)
      maximumImportRss = Math.max(maximumImportRss, sampledRss);
    const at = performance.now();
    manager.status();
    statusMax = Math.max(statusMax, performance.now() - at);
  }, 10);
  const importStartRss = process.memoryUsage().rss;
  maximumImportRss = importStartRss;
  measuringImport = true;
  const at = performance.now();
  while (manager.status()[0].job?.state === "running") {
    assert.ok(performance.now() - at < 240000);
    await manager.tick();
    await new Promise((r) => setTimeout(r, 10));
  }
  const elapsed = performance.now() - at;
  const importEndRss = process.memoryUsage().rss;
  maximumImportRss = Math.max(maximumImportRss, importEndRss);
  measuringImport = false;
  assert.equal(manager.status()[0].job?.state, "completed");
  let rows: any[] = [];
  for (let i = 0; i < 100; i++) {
    rows = (await sql(`select value,time from ${table}`)).dataset;
    if (rows.length === count) break;
    await new Promise((r) => setTimeout(r, 100));
  }
  assert.equal(rows.length, count);
  const seen = new Set<number>();
  for (const [id, time] of rows) {
    assert.ok(Number.isInteger(id) && id >= 0 && id < count && !seen.has(id));
    assert.equal(Date.parse(time), eventTime(id));
    seen.add(id);
  }
  const latest = (
    await sql(`select value,time from ${table} order by time desc limit 1`)
  ).dataset[0];
  const latestId = timestampFixture === "reverse-days" ? 0 : count - 1;
  assert.equal(latest[0], latestId);
  assert.equal(Date.parse(latest[1]), eventTime(latestId));
  assert.equal(
    (await fs.readdir(source)).filter((n) => n.endsWith(".event")).length,
    0,
  );
  const evidence = {
    scope: "real-files-and-QuestDB-modules",
    excludes: ["full-runtime", "production-scale"],
    profile,
    settings,
    timestampFixture,
    eventCount: count,
    elapsedImportMs: elapsed,
    filesPerSecond: count / (elapsed / 1000),
    verifiedRows: seen.size,
    duplicateRows: 0,
    timestampMismatches: 0,
    latestValueAndTimeVerified: true,
    importStartRssBytes: importStartRss,
    importEndRssBytes: importEndRss,
    maximumSampledImportRssBytes: maximumImportRss,
    maximumSampledRssBytes: Math.max(rss, process.memoryUsage().rss),
    eventLoopDelayMaxMs: loop.max / 1e6,
    statusHelperMaxMs: statusMax,
    writer: writer.getBatchDiagnostics(),
    importer: manager.status()[0],
  };
  if (output)
    await fs.writeFile(output, JSON.stringify(evidence, null, 2) + "\n");
  console.log(JSON.stringify(evidence));
} finally {
  clearInterval(sample);
  loop.disable();
  await manager.close();
  await writer.close();
  await sql(`drop table if exists ${table}`);
  await fs.rm(root, { recursive: true, force: true });
}
