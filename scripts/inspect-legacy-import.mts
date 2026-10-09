import { promises as fs } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import { performance, monitorEventLoopDelay } from "node:perf_hooks";
import type { Sender } from "@questdb/nodejs-client";
import { LegacyImportManager } from "../src/legacy-import.js";
import { writeLegacyEvent } from "../src/legacy-import-writer.js";
import { QuestDBWriter } from "../src/writers/questDbWriter.js";

const eventCount = Number(process.argv[2] ?? 20_000);
if (!Number.isInteger(eventCount) || eventCount < 1 || eventCount > 100_000)
  throw new Error("Use 1..100000 synthetic events");
const root = await fs.realpath(
  await fs.mkdtemp(path.join(tmpdir(), "archiver-legacy-inspection-")),
);
const source = path.join(root, "old");
const live = path.join(root, "live");
await fs.mkdir(source);
await fs.mkdir(live);
const expectedStart = Date.parse("2026-07-19T12:00:00.000Z");
const committed = new Map<number, number>(); // Fixture oracle only; not part of importer memory.
let flushes = 0;
let committedWrites = 0;
let current: { id: number; timestamp: number } | null = null;
let rows: { id: number; timestamp: number }[] = [];
class SyntheticSender {
  table() {
    current = { id: 0, timestamp: 0 };
    return this;
  }
  symbol() {
    return this;
  }
  booleanColumn() {
    return this;
  }
  timestampColumn() {
    return this;
  }
  stringColumn() {
    return this;
  }
  floatColumn(name: string, value: number) {
    if (name === "value") current!.id = value;
    return this;
  }
  async at(time: number) {
    current!.timestamp = time;
    rows.push(current!);
  }
  async flush() {
    await new Promise((resolve) => setTimeout(resolve, 2));
    for (const row of rows) {
      committed.set(row.id, row.timestamp);
      committedWrites++;
    }
    rows = [];
    flushes++;
  }
  reset() {
    rows = [];
  }
  async close() {}
}
const writer = new QuestDBWriter(
  new SyntheticSender() as unknown as Sender,
  undefined,
  { flushIntervalMs: 10, maxRows: 64, maxPendingRows: 256 },
);
let inFlight = 0;
let maximumInFlight = 0;
const manager = new LegacyImportManager({
  sources: [{ id: "old", directory: source }],
  liveDirectory: live,
  instanceId: "synthetic-inspection",
  policyDigest: () => "neutral-append-policy",
  settings: {
    batchSize: 512,
    concurrency: 64,
    intervalMs: 100,
    maxFileBytes: 1024,
    maxBatchBytes: 1024 * 1024,
  },
  canWrite: () => true,
  hasLiveHeadroom: () => true,
  write: async (event) => {
    inFlight++;
    maximumInFlight = Math.max(inFlight, maximumInFlight);
    try {
      return await writeLegacyEvent(event, {
        findStorage: () => ({ ingestMode: "append" }),
        write: (packet) =>
          writer.writeUnsPacket(packet, "uns_fixture", event.topic),
      });
    } finally {
      inFlight--;
    }
  },
});
const loop = monitorEventLoopDelay({ resolution: 10 });
try {
  for (let index = 0; index < eventCount; index += 32) {
    await Promise.all(
      Array.from(
        { length: Math.min(32, eventCount - index) },
        async (_, offset) => {
          const id = index + offset;
          const message = {
            version: "2.0.0",
            message: {
              data: {
                value: id,
                time: new Date(expectedStart + id).toISOString(),
              },
            },
          };
          await fs.writeFile(
            path.join(
              source,
              `${id}.event${id % 1000 === 0 ? ".99999999.100.processing" : ""}`,
            ),
            JSON.stringify({
              topic: "plant/legacy/equipment/motor/speed",
              message: JSON.stringify(message),
            }),
          );
        },
      ),
    );
  }
  manager.inspect("old");
  let inventoryPasses = 0;
  while (
    !manager.status()[0].inspection.scanComplete &&
    inventoryPasses < 2000
  ) {
    await manager.tick();
    inventoryPasses++;
  }
  const inventory = manager.status()[0].inspection;
  const statusTimes: number[] = [];
  for (let index = 0; index < 10_000; index++) {
    const start = performance.now();
    JSON.stringify(manager.status());
    statusTimes.push(performance.now() - start);
  }
  statusTimes.sort((a, b) => a - b);
  await manager.command({
    action: "start",
    sourceId: "old",
    expectedRevision: 0,
    requestId: "inspect-start",
    confirmSourceClosed: true,
  });
  const started = performance.now();
  loop.enable();
  let passes = 0;
  let maximumRss = process.memoryUsage().rss;
  while (
    manager.status()[0].job!.state === "running" &&
    performance.now() - started < 180_000
  ) {
    await manager.tick();
    passes++;
    maximumRss = Math.max(maximumRss, process.memoryUsage().rss);
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  loop.disable();
  const job = manager.status()[0].job!;
  const timestampMismatches = [...committed.entries()].filter(
    ([id, time]) => time !== expectedStart + id,
  ).length;
  console.log(
    JSON.stringify(
      {
        scope:
          "physical-local-files-real-importer-and-QuestDBWriter-with-synthetic-ILP-ACK-not-real-QuestDB",
        eventCount,
        inventoryPasses,
        inventory,
        job,
        passes,
        flushes,
        maximumInFlight,
        committedRows: committed.size,
        committedWrites,
        duplicateCommittedRows: committedWrites - committed.size,
        timestampMismatches,
        durationMs: performance.now() - started,
        maximumRssBytes: maximumRss,
        eventLoopDelayMaxMs: loop.max / 1e6,
        statusHelperCalls: 10000,
        statusHelperP99Ms: statusTimes[9900],
      },
      null,
      2,
    ),
  );
  if (
    job.state !== "completed" ||
    committed.size !== eventCount ||
    committedWrites !== eventCount ||
    timestampMismatches
  )
    process.exitCode = 1;
} finally {
  await manager.close();
  await writer.close();
  loop.disable();
  await fs.rm(root, { recursive: true, force: true });
}
