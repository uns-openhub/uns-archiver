import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { StoredEventReplay } from "../src/stored-event-replay.js";

const deferred = <T = void>() => {
  let resolve: (value: T | PromiseLike<T>) => void = () => undefined;
  const promise = new Promise<T>((nextResolve) => {
    resolve = nextResolve;
  });
  return { promise, resolve };
};

const waitFor = async (condition: () => boolean): Promise<void> => {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    if (condition()) return;
    await new Promise<void>((resolve) => setTimeout(resolve, 10));
  }
  assert.fail("Timed out while waiting for replay work.");
};

const createWorkspace = async (t: test.TestContext) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "uns-archiver-replay-"));
  const events = path.join(root, "event_storage");
  const failed = path.join(events, "failed");
  await fs.mkdir(failed, { recursive: true });
  const replays: StoredEventReplay[] = [];
  t.after(async () => {
    await Promise.all(replays.map((replay) => replay.close()));
    await fs.rm(root, { recursive: true, force: true });
  });
  return { events, failed, replays };
};

const writeEvent = async (
  events: string,
  name: string,
  value: unknown = { id: name },
) => {
  await fs.writeFile(path.join(events, `${name}.event`), JSON.stringify(value));
};

const createReplay = (
  directories: {
    events: string;
    failed: string;
    replays?: StoredEventReplay[];
  },
  options: {
    processEvent: (event: unknown) => Promise<boolean>;
    ready?: () => boolean;
    headroom?: () => boolean;
    batchSize?: number;
    concurrency?: number;
    currentProcessId?: number;
    scanMaxEntries?: number;
  },
) => {
  const replay = new StoredEventReplay({
    eventStorageDirectory: directories.events,
    failedStorageDirectory: directories.failed,
    eventFileExtension: ".event",
    processingExtension: ".processing",
    currentProcessId: options.currentProcessId,
    isReady: options.ready ?? (() => true),
    isStopping: () => false,
    hasLiveHeadroom: options.headroom ?? (() => true),
    getLimits: () => ({
      batchSize: options.batchSize ?? 64,
      concurrency: options.concurrency ?? 8,
    }),
    processEvent: options.processEvent,
    getScanLimits: () => ({
      maxEntries: options.scanMaxEntries ?? 512,
      maxDurationMs: 25,
    }),
  });
  directories.replays?.push(replay);
  return replay;
};

test("replays while live work remains pending when its reserved headroom is available", async (t) => {
  const directories = await createWorkspace(t);
  const processed: string[] = [];
  const replay = createReplay(directories, {
    // Represents a continuously non-empty live queue below its reserve limit.
    headroom: () => true,
    processEvent: async (event) => {
      processed.push((event as { id: string }).id);
      return true;
    },
  });
  await writeEvent(directories.events, "one");
  await writeEvent(directories.events, "two");

  await replay.run();

  assert.deepEqual(processed.sort(), ["one", "two"]);
  assert.equal(await replay.countQueued(), 0);
  const diagnostics = replay.diagnostics(0);
  assert.equal(diagnostics.successful, 2);
  assert.equal(diagnostics.active, false);
  assert.notEqual(diagnostics.lastRunAt, null);
  assert.notEqual(diagnostics.lastSuccessAt, null);
});

test("bounds a health backlog sample and reports a completed small-directory observation", async (t) => {
  const directories = await createWorkspace(t);
  const replay = createReplay(directories, { processEvent: async () => true });
  for (let index = 0; index < 5; index += 1) {
    await writeEvent(directories.events, `event-${index}`);
  }
  await fs.writeFile(
    path.join(directories.events, "unfinished.tmp"),
    "partial",
  );

  assert.equal(await replay.countQueuedUpTo(3), 3);
  assert.equal(await replay.countQueued(), 5);
});

test("returns an unknown sample when event storage cannot be scanned", async (t) => {
  const directories = await createWorkspace(t);
  const filePath = path.join(directories.events, "not-a-directory");
  await fs.writeFile(filePath, "file");
  const replay = createReplay(
    { events: filePath, failed: directories.failed },
    {
      processEvent: async () => true,
    },
  );
  assert.equal(await replay.countQueuedUpTo(3), null);
});

test("keeps durable work queued when live headroom is exhausted", async (t) => {
  const directories = await createWorkspace(t);
  let hasHeadroom = false;
  let processed = 0;
  const replay = createReplay(directories, {
    headroom: () => hasHeadroom,
    processEvent: async () => {
      processed += 1;
      return true;
    },
  });
  await writeEvent(directories.events, "reserved");

  await replay.run();
  assert.equal(processed, 0);
  assert.equal(await replay.countQueued(), 1);

  hasHeadroom = true;
  await replay.run();
  assert.equal(processed, 1);
  assert.equal(await replay.countQueued(), 0);
});

test("persists event mutations before requeueing a retry", async (t) => {
  const directories = await createWorkspace(t);
  const replay = createReplay(directories, {
    processEvent: async (event) => {
      (event as Record<string, unknown>).identity = { status: "retry" };
      return false;
    },
  });
  await writeEvent(directories.events, "identity", { id: "identity" });

  await replay.run();

  const stored = JSON.parse(
    await fs.readFile(path.join(directories.events, "identity.event"), "utf8"),
  ) as Record<string, unknown>;
  assert.deepEqual(stored.identity, { status: "retry" });
  assert.equal(replay.diagnostics(1).requeued, 1);
});

test("bounds replay batch size and runs only the configured concurrent writes", async (t) => {
  const directories = await createWorkspace(t);
  const gate = deferred();
  let started = 0;
  let active = 0;
  let maxActive = 0;
  const replay = createReplay(directories, {
    batchSize: 5,
    concurrency: 2,
    processEvent: async () => {
      started += 1;
      active += 1;
      maxActive = Math.max(maxActive, active);
      if (started <= 2) await gate.promise;
      active -= 1;
      return true;
    },
  });
  for (let index = 0; index < 7; index += 1) {
    await writeEvent(directories.events, `event-${index}`);
  }

  const run = replay.run();
  await waitFor(() => started === 2);
  assert.equal(maxActive, 2);
  assert.equal(replay.diagnostics(await replay.countQueued()).inFlight, 2);

  gate.resolve();
  await run;
  assert.equal(started, 5);
  assert.equal(maxActive, 2);
  assert.equal(await replay.countQueued(), 2);
});

test("deletes only confirmed events and atomically requeues retryable work", async (t) => {
  const directories = await createWorkspace(t);
  let successful = false;
  const replay = createReplay(directories, {
    processEvent: async () => successful,
  });
  await writeEvent(directories.events, "retry");

  await replay.run();
  assert.equal(await replay.countQueued(), 1);
  assert.equal(replay.diagnostics(1).requeued, 1);
  assert.equal(
    (await fs.readdir(directories.events)).some((name) =>
      name.endsWith(".processing"),
    ),
    false,
  );

  successful = true;
  await replay.run();
  assert.equal(await replay.countQueued(), 0);
  assert.equal(replay.diagnostics(0).successful, 1);
});

test("moves malformed stored files aside without treating them as confirmed writes", async (t) => {
  const directories = await createWorkspace(t);
  let processed = 0;
  const replay = createReplay(directories, {
    processEvent: async () => {
      processed += 1;
      return true;
    },
  });
  await fs.writeFile(
    path.join(directories.events, "malformed.event"),
    "not-json",
  );

  await replay.run();

  assert.equal(processed, 0);
  assert.equal(await replay.countQueued(), 0);
  assert.equal((await fs.readdir(directories.failed)).length, 1);
  assert.equal(replay.diagnostics(0).failed, 1);
});

test("does not replay before active-topic readiness succeeds", async (t) => {
  const directories = await createWorkspace(t);
  let ready = false;
  let processed = 0;
  const replay = createReplay(directories, {
    ready: () => ready,
    processEvent: async () => {
      processed += 1;
      return true;
    },
  });
  await writeEvent(directories.events, "not-ready");

  await replay.run();
  assert.equal(processed, 0);
  assert.equal(replay.diagnostics(1).lastRunAt, null);

  ready = true;
  await replay.run();
  assert.equal(processed, 1);
});

test("does not start a new replay pass after shutdown begins", async (t) => {
  const directories = await createWorkspace(t);
  let stopping = true;
  let processed = 0;
  const replay = new StoredEventReplay({
    eventStorageDirectory: directories.events,
    failedStorageDirectory: directories.failed,
    eventFileExtension: ".event",
    processingExtension: ".processing",
    isReady: () => true,
    isStopping: () => stopping,
    hasLiveHeadroom: () => true,
    getLimits: () => ({ batchSize: 1, concurrency: 1 }),
    processEvent: async () => {
      processed += 1;
      return true;
    },
  });
  directories.replays.push(replay);
  await writeEvent(directories.events, "shutdown");

  await replay.run();
  assert.equal(processed, 0);
  assert.equal(await replay.countQueued(), 1);

  stopping = false;
  await replay.run();
  assert.equal(processed, 1);
});

test("recovers stale processing files but leaves this process's active locks alone", async (t) => {
  const directories = await createWorkspace(t);
  const currentProcessId = 424_242;
  const replay = createReplay(directories, {
    currentProcessId,
    processEvent: async () => true,
  });
  await fs.writeFile(
    path.join(directories.events, "stale.event.999999999.1.processing"),
    JSON.stringify({ id: "stale" }),
  );
  await fs.writeFile(
    path.join(
      directories.events,
      `active.event.${currentProcessId}.1.processing`,
    ),
    JSON.stringify({ id: "active" }),
  );

  await replay.recoverStaleProcessing();

  assert.equal(
    await fs.readFile(path.join(directories.events, "stale.event"), "utf8"),
    JSON.stringify({ id: "stale" }),
  );
  assert.equal(
    await fs
      .access(
        path.join(
          directories.events,
          `active.event.${currentProcessId}.1.processing`,
        ),
      )
      .then(() => true),
    true,
  );
  assert.equal(
    replay.diagnostics(await replay.countQueued()).recoveredStaleProcessing,
    1,
  );
});

test("quarantines an unrecognizable processing file instead of leaving it invisible", async (t) => {
  const directories = await createWorkspace(t);
  const replay = createReplay(directories, {
    processEvent: async () => true,
  });
  await fs.writeFile(
    path.join(directories.events, "orphan.processing"),
    "not-json",
  );

  await replay.recoverStaleProcessing();

  assert.equal(
    (await fs.readdir(directories.events)).includes("orphan.processing"),
    false,
  );
  assert.equal((await fs.readdir(directories.failed)).length, 1);
  assert.equal(replay.diagnostics(0).failed, 1);
});

test("coalesces overlapping timer, refresh, and manual replay requests", async (t) => {
  const directories = await createWorkspace(t);
  const gate = deferred();
  let started = 0;
  const replay = createReplay(directories, {
    processEvent: async () => {
      started += 1;
      await gate.promise;
      return true;
    },
  });
  await writeEvent(directories.events, "single-run");

  const timerRun = replay.run();
  const refreshRun = replay.run();
  const manualRun = replay.run();
  await waitFor(() => started === 1);
  assert.equal(started, 1);

  gate.resolve();
  await Promise.all([timerRun, refreshRun, manualRun]);
  assert.equal(started, 1);
});

test("inventory is bounded by all entries, and partial zero is not an empty queue", async (t) => {
  const directories = await createWorkspace(t);
  for (let i = 0; i < 12; i++)
    await fs.writeFile(path.join(directories.events, `${i}.tmp`), "partial");
  await writeEvent(directories.events, "queued");
  const replay = createReplay(directories, {
    processEvent: async () => true,
    scanMaxEntries: 2,
  });
  assert.equal(replay.snapshot().countKind, "unknown");
  assert.equal(await replay.countQueued(), null);
  assert.equal(replay.snapshot().scanComplete, false);
  assert.equal(replay.snapshot().entriesVisited, 2);
  assert.equal(await replay.countQueuedUpTo(1000), null);
  for (let i = 0; i < 20 && !replay.snapshot().scanComplete; i++) {
    const before = replay.snapshot().entriesVisited;
    await replay.refreshQueueSnapshot();
    assert.ok(replay.snapshot().entriesVisited - before <= 2);
  }
  assert.equal(replay.snapshot().countKind, "observed");
  assert.equal(replay.snapshot().queuedEvents, 1);
  assert.equal(replay.snapshot().otherEntries, 13); // includes failed directory
  assert.equal(replay.snapshot().entriesVisited, 14);
});

test("cached snapshots neither open storage nor conceal an inspection error", async (t) => {
  const directories = await createWorkspace(t);
  const replay = createReplay(directories, { processEvent: async () => true });
  await replay.refreshQueueSnapshot();
  const cached = replay.snapshot();
  assert.equal(cached.queuedEvents, 0);
  await fs.rename(directories.events, `${directories.events}-original`);
  await fs.writeFile(directories.events, "not a directory");
  for (let i = 0; i < 100; i++) assert.deepEqual(replay.snapshot(), cached);
  cached.queuedEvents = 999;
  assert.equal(replay.snapshot().queuedEvents, 0);
  await replay.refreshQueueSnapshot();
  assert.equal(replay.snapshot().queuedEvents, null);
  assert.equal(replay.snapshot().countKind, "unknown");
  assert.equal(replay.snapshot().scanComplete, false);
  assert.equal(replay.snapshot().lastError, "stored-replay-count-failed");
});

test("counts processing files separately rather than presenting a zero as a drained spool", async (t) => {
  const directories = await createWorkspace(t);
  await fs.writeFile(
    path.join(
      directories.events,
      `interrupted.event.${process.pid}.1.processing`,
    ),
    "{}",
  );
  const replay = createReplay(directories, { processEvent: async () => true });
  await replay.refreshQueueSnapshot();
  assert.equal(replay.snapshot().queuedEvents, 0);
  assert.equal(replay.snapshot().processingFiles, 1);
  assert.equal(replay.snapshot().unresolvedProcessingFiles, 1);
  assert.equal(replay.snapshot().scanComplete, true);
});

test("replay makes forward progress past nonmatching entries over several bounded passes", async (t) => {
  const directories = await createWorkspace(t);
  for (let i = 0; i < 20; i++)
    await fs.writeFile(path.join(directories.events, `${i}.tmp`), "partial");
  await writeEvent(directories.events, "valid");
  let processed = 0;
  const replay = createReplay(directories, {
    processEvent: async () => {
      processed++;
      return true;
    },
    scanMaxEntries: 2,
  });
  for (let i = 0; i < 30 && processed === 0; i++) await replay.run();
  assert.equal(processed, 1);
  assert.equal(replay.diagnostics().successful, 1);
});

test("recovery advances in bounded passes and preserves conflicting processing content", async (t) => {
  const directories = await createWorkspace(t);
  for (let i = 0; i < 7; i++) {
    await fs.writeFile(
      path.join(directories.events, `stale-${i}.event.999999999.1.processing`),
      JSON.stringify({ id: i }),
    );
  }
  await writeEvent(directories.events, "conflict", { original: true });
  const conflict = path.join(
    directories.events,
    "conflict.event.999999999.1.processing",
  );
  await fs.writeFile(conflict, JSON.stringify({ distinctProcessing: true }));
  const replay = createReplay(directories, {
    processEvent: async () => true,
    scanMaxEntries: 2,
  });
  await replay.recoverStaleProcessing();
  assert.equal(replay.diagnostics().processingRecovery.scanComplete, false);
  assert.equal(replay.diagnostics().processingRecovery.entriesVisited, 2);
  for (
    let i = 0;
    i < 20 && !replay.diagnostics().processingRecovery.scanComplete;
    i++
  ) {
    const before = replay.diagnostics().processingRecovery.entriesVisited;
    await replay.recoverStaleProcessing();
    assert.ok(
      replay.diagnostics().processingRecovery.entriesVisited - before <= 2,
    );
  }
  assert.equal(replay.diagnostics().recoveredStaleProcessing, 7);
  assert.equal(
    await fs.readFile(conflict, "utf8"),
    JSON.stringify({ distinctProcessing: true }),
  );
  assert.equal(
    JSON.parse(
      await fs.readFile(
        path.join(directories.events, "conflict.event"),
        "utf8",
      ),
    ).original,
    true,
  );
});

test("requeue preserves both files if another durable file exists", async (t) => {
  const directories = await createWorkspace(t);
  const replay = createReplay(directories, {
    processEvent: async () => {
      await writeEvent(directories.events, "conflict", { distinct: true });
      return false;
    },
  });
  await writeEvent(directories.events, "conflict", { original: true });
  await replay.run();
  const names = await fs.readdir(directories.events);
  assert.equal(names.filter((name) => name.endsWith(".processing")).length, 1);
  assert.equal(
    JSON.parse(
      await fs.readFile(
        path.join(directories.events, "conflict.event"),
        "utf8",
      ),
    ).distinct,
    true,
  );
  assert.equal(
    replay.diagnostics().lastError,
    "stored-replay-requeue-conflict",
  );
});

test("background maintenance starts without awaiting recovery and closes on shutdown", async (t) => {
  const directories = await createWorkspace(t);
  for (let i = 0; i < 8; i++) await writeEvent(directories.events, `${i}`);
  const replay = createReplay(directories, {
    processEvent: async () => true,
    scanMaxEntries: 2,
  });
  replay.startBackgroundMaintenance();
  replay.startBackgroundMaintenance(); // idempotent, not a second scanner loop
  assert.equal(replay.snapshot().capturedAt, null);
  await waitFor(() => replay.snapshot().entriesVisited >= 2);
  await replay.close();
  const snapshot = replay.snapshot();
  await new Promise((resolve) => setTimeout(resolve, 150));
  assert.deepEqual(replay.snapshot(), snapshot);
  assert.equal(replay.diagnostics().active, false);
  replay.startBackgroundMaintenance(); // closed scanners do not restart
  await replay.run();
  assert.equal(replay.diagnostics().successful, 0);
});

test("a permanently deferred batch does not repeatedly hide later eligible events", async (t) => {
  const directories = await createWorkspace(t);
  for (let i = 0; i < 10; i++)
    await writeEvent(directories.events, `retry-${i}`, { retry: true });
  await writeEvent(directories.events, "valid", { valid: true });
  let succeeded = 0;
  const replay = createReplay(directories, {
    batchSize: 1,
    scanMaxEntries: 2,
    processEvent: async (event) => {
      if ((event as { valid?: boolean }).valid) {
        succeeded++;
        return true;
      }
      return false;
    },
  });
  for (let i = 0; i < 20 && !succeeded; i++) await replay.run();
  assert.equal(succeeded, 1);
  assert.ok(replay.diagnostics().requeued <= 10);
});

test("inventory distinguishes this replay's active writer from an unresolved processing file", async (t) => {
  const directories = await createWorkspace(t);
  const gate = deferred();
  let started = false;
  const replay = createReplay(directories, {
    processEvent: async () => {
      started = true;
      await gate.promise;
      return true;
    },
  });
  await writeEvent(directories.events, "live-write");
  const run = replay.run();
  try {
    await waitFor(() => started);
    await fs.writeFile(
      path.join(
        directories.events,
        `unknown.event.${process.pid}.1.processing`,
      ),
      "{}",
    );
    await replay.refreshQueueSnapshot();
    assert.equal(replay.snapshot().processingFiles, 2);
    assert.equal(replay.snapshot().unresolvedProcessingFiles, 1);
  } finally {
    gate.resolve();
    await run;
  }
});
