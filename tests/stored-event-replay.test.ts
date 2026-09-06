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
  t.after(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });
  return { events, failed };
};

const writeEvent = async (
  events: string,
  name: string,
  value: unknown = { id: name },
) => {
  await fs.writeFile(path.join(events, `${name}.event`), JSON.stringify(value));
};

const createReplay = (
  directories: { events: string; failed: string },
  options: {
    processEvent: (event: unknown) => Promise<boolean>;
    ready?: () => boolean;
    headroom?: () => boolean;
    batchSize?: number;
    concurrency?: number;
    currentProcessId?: number;
  },
) =>
  new StoredEventReplay({
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
  });

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
