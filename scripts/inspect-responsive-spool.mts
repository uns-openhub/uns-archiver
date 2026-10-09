/** Local synthetic filesystem check. Never reads a caller-supplied spool directory. */
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance, monitorEventLoopDelay } from "node:perf_hooks";
import { StoredEventReplay } from "../src/stored-event-replay.js";
import { storedReplayControlStatus } from "../src/stored-replay-control.js";

const root = await fs.mkdtemp(
  path.join(os.tmpdir(), "archiver-responsive-inspection-"),
);
const events = path.join(root, "events");
await fs.mkdir(path.join(events, "failed"), { recursive: true });
const lag = monitorEventLoopDelay({ resolution: 10 });
lag.enable();
const originalOpen = fs.opendir.bind(fs);
let opens = 0,
  reads = 0,
  closes = 0,
  acknowledged = 0;
fs.opendir = (async (...args: Parameters<typeof fs.opendir>) => {
  const cursor = await originalOpen(...args);
  opens++;
  return {
    read: async () => {
      reads++;
      return await cursor.read();
    },
    close: async () => {
      closes++;
      await cursor.close();
    },
  };
}) as typeof fs.opendir;
const replay = new StoredEventReplay({
  eventStorageDirectory: events,
  failedStorageDirectory: path.join(events, "failed"),
  eventFileExtension: ".event",
  processingExtension: ".processing",
  isReady: () => true,
  isStopping: () => false,
  hasLiveHeadroom: () => true,
  getLimits: () => ({ batchSize: 256, concurrency: 64 }),
  processEvent: async () => {
    acknowledged++;
    return true;
  },
});
try {
  let next = 0;
  await Promise.all(
    Array.from({ length: 32 }, async () => {
      while (next < 20_000) {
        const index = next++;
        await fs.writeFile(
          path.join(events, `${String(index).padStart(8, "0")}.tmp`),
          "synthetic incomplete file",
        );
      }
    }),
  );
  for (let i = 0; i < 4; i++)
    await fs.writeFile(
      path.join(events, `queued-${i}.event`),
      JSON.stringify({ id: i }),
    );
  const started = performance.now();
  const opensBefore = opens;
  const firstReadsBefore = reads;
  await replay.refreshQueueSnapshot();
  const firstPassMs = performance.now() - started;
  const firstPassReads = reads - firstReadsBefore;
  assert.ok(firstPassReads <= 512);
  assert.equal(replay.snapshot().scanComplete, false);
  assert.equal(await replay.countQueuedUpTo(1000), null);
  const readsBeforeStatus = reads;
  const times: number[] = [];
  for (let i = 0; i < 10_000; i++) {
    const before = performance.now();
    JSON.stringify(
      storedReplayControlStatus(undefined, {
        getPaused: () => false,
        setPaused: () => {},
        requestReplay: () => {},
        snapshot: () => replay.snapshot(),
      }),
    );
    times.push(performance.now() - before);
  }
  const statusFilesystemReads = reads - readsBeforeStatus;
  assert.equal(statusFilesystemReads, 0);
  let passes = 2;
  while (!replay.snapshot().scanComplete) {
    const before = reads;
    await replay.refreshQueueSnapshot();
    assert.ok(reads - before <= 513); // last pass may read EOF after its entries
    assert.ok(++passes < 1000);
  }
  assert.equal(opens - opensBefore, 1);
  assert.equal(replay.snapshot().queuedEvents, 4);
  assert.equal(replay.snapshot().otherEntries, 20_001);
  assert.equal(replay.snapshot().entriesVisited, 20_005);
  const inventory = replay.snapshot();
  for (let i = 0; i < 2; i++) {
    await fs.writeFile(
      path.join(events, `recover-${i}.event.999999999.1.processing`),
      JSON.stringify({ id: `recover-${i}` }),
    );
  }
  const recoveryReadsBefore = reads;
  await replay.recoverStaleProcessing();
  const firstRecoveryPassReads = reads - recoveryReadsBefore;
  assert.ok(firstRecoveryPassReads <= 512);
  assert.equal(replay.diagnostics().processingRecovery.scanComplete, false);
  let recoveryPasses = 1;
  while (!replay.diagnostics().processingRecovery.scanComplete) {
    const before = reads;
    await replay.recoverStaleProcessing();
    assert.ok(reads - before <= 513);
    assert.ok(++recoveryPasses < 1000);
  }
  assert.equal(replay.diagnostics().recoveredStaleProcessing, 2);
  const recovery = {
    passes: recoveryPasses,
    firstPassEntries: firstRecoveryPassReads,
    recovered: 2,
    ...replay.diagnostics().processingRecovery,
  };
  let replayPasses = 0;
  while (acknowledged < 6) {
    const before = reads;
    await replay.run();
    assert.ok(reads - before <= 513);
    assert.ok(++replayPasses < 1000);
  }
  assert.equal(replay.diagnostics().successful, 6);
  await replay.close();
  assert.equal(opens, closes);
  times.sort((a, b) => a - b);
  lag.disable();
  console.log(
    JSON.stringify(
      {
        date: "2026-10-09",
        kind: "20k physical nonmatching entries plus four events, real scanner/replay with fake writer acknowledgements; no MQTT, HTTP auth or QuestDB; two dead-owner processing fixtures added after inventory",
        firstPass: {
          entriesVisited: firstPassReads,
          durationMs: Number(firstPassMs.toFixed(3)),
        },
        inventory: { passes, opens: 1, ...inventory },
        cachedStatus: {
          calls: 10_000,
          filesystemReads: statusFilesystemReads,
          p99Ms: Number(times[9899].toFixed(4)),
          maxMs: Number(times.at(-1)!.toFixed(4)),
        },
        replay: {
          passes: replayPasses,
          fakeAcknowledged: acknowledged,
          successful: 6,
        },
        recovery,
        resources: {
          peakRssMiB: Math.round(process.resourceUsage().maxRSS / 1024),
          eventLoopDelayMaxMs: Number((lag.max / 1e6).toFixed(3)),
          cursorsOpened: opens,
          cursorsClosed: closes,
        },
      },
      null,
      2,
    ),
  );
} finally {
  await replay.close();
  fs.opendir = originalOpen as typeof fs.opendir;
  lag.disable();
  await fs.rm(root, { recursive: true, force: true });
}
