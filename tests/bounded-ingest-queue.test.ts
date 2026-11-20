import assert from "node:assert/strict";
import test from "node:test";
import { BoundedIngestQueue } from "../src/bounded-ingest-queue.js";

const deferred = <T = void>() => {
  let resolve: (value: T | PromiseLike<T>) => void = () => undefined;
  const promise = new Promise<T>((nextResolve) => {
    resolve = nextResolve;
  });
  return { promise, resolve };
};

test("bounds live work and spills overflow without retaining duplicate events", async () => {
  const first = deferred();
  const processed: string[] = [];
  const spilled: string[] = [];
  const queue = new BoundedIngestQueue<string>({
    maxPendingEvents: 2,
    maxPendingBytes: 20,
    concurrency: 1,
    process: async (item) => {
      processed.push(item.value);
      if (item.value === "first") await first.promise;
    },
    spill: async (item) => {
      spilled.push(item.value);
    },
  });

  assert.equal(queue.enqueue({ id: "1", value: "first", bytes: 5 }), "queued");
  assert.equal(queue.enqueue({ id: "2", value: "second", bytes: 5 }), "queued");
  assert.equal(
    queue.enqueue({ id: "3", value: "overflow", bytes: 5 }),
    "spilled",
  );
  assert.equal(
    queue.enqueue({ id: "3", value: "overflow", bytes: 5 }),
    "duplicate",
  );

  assert.deepEqual(queue.snapshot(), {
    pendingEvents: 2,
    pendingBytes: 10,
    queuedEvents: 1,
    processingEvents: 1,
    spillingEvents: 1,
    overflowSpilled: 1,
    duplicateSuppressed: 1,
    processFailures: 0,
    limits: { maxPendingEvents: 2, maxPendingBytes: 20, concurrency: 1 },
  });

  first.resolve();
  await queue.waitForIdle();
  assert.deepEqual(processed, ["first", "second"]);
  assert.deepEqual(spilled, ["overflow"]);
  assert.equal(queue.snapshot().pendingEvents, 0);
  assert.equal(queue.snapshot().pendingBytes, 0);
});

test("spills an item that would exceed the byte limit", async () => {
  const spilled: string[] = [];
  const queue = new BoundedIngestQueue<string>({
    maxPendingEvents: 5,
    maxPendingBytes: 4,
    concurrency: 1,
    process: async () => undefined,
    spill: async (item) => {
      spilled.push(item.value);
    },
  });

  assert.equal(
    queue.enqueue({ id: "large", value: "large", bytes: 5 }),
    "spilled",
  );
  await queue.waitForIdle();
  assert.deepEqual(spilled, ["large"]);
  assert.equal(queue.snapshot().overflowSpilled, 1);
});

test("waits for active ingest and durable spills before shutdown completes", async () => {
  const activeIngest = deferred();
  const durableSpill = deferred();
  const queue = new BoundedIngestQueue<string>({
    maxPendingEvents: 1,
    maxPendingBytes: 20,
    concurrency: 1,
    process: async () => {
      await activeIngest.promise;
    },
    spill: async () => {
      await durableSpill.promise;
    },
  });

  assert.equal(
    queue.enqueue({ id: "active", value: "active", bytes: 5 }),
    "queued",
  );
  assert.equal(
    queue.enqueue({ id: "spill", value: "spill", bytes: 5 }),
    "spilled",
  );

  const idle = queue.waitForIdle();
  let completed = false;
  void idle.then(() => {
    completed = true;
  });

  await Promise.resolve();
  assert.equal(completed, false);

  activeIngest.resolve();
  await Promise.resolve();
  assert.equal(completed, false);

  durableSpill.resolve();
  await idle;
  assert.equal(completed, true);
});

test("persists an unexpected live-processing failure and releases queue capacity", async () => {
  const spills: Array<{ value: string; reason: string }> = [];
  const queue = new BoundedIngestQueue<string>({
    maxPendingEvents: 1,
    maxPendingBytes: 20,
    concurrency: 1,
    process: async () => {
      throw new Error("QuestDB write failed before event handling");
    },
    spill: async (item, reason) => {
      spills.push({ value: item.value, reason });
    },
  });

  assert.equal(
    queue.enqueue({ id: "failed", value: "failed", bytes: 5 }),
    "queued",
  );
  await queue.waitForIdle();

  assert.deepEqual(spills, [{ value: "failed", reason: "process_error" }]);
  assert.equal(queue.snapshot().processFailures, 1);
  assert.equal(queue.snapshot().pendingEvents, 0);
  assert.equal(queue.snapshot().pendingBytes, 0);
});

test("handover preserves waiting occurrence IDs without copying an active writer", async () => {
  const release = deferred();
  const started: string[] = [];
  const persisted: string[] = [];
  const queue = new BoundedIngestQueue<number>({
    maxPendingEvents: 4, maxPendingBytes: 40, concurrency: 1,
    process: async item => { started.push(item.id); await release.promise; },
    spill: async () => assert.fail("No ordinary overflow was expected"),
  });
  for (let i = 1; i <= 3; i++) queue.enqueue({ id: "event-" + i, value: i, bytes: 10 });
  assert.throws(() => queue.persistWaiting(item => {
    if (item.id === "event-3") throw Error("Disk unavailable");
    persisted.push(item.id);
  }), /Disk unavailable/);
  assert.deepEqual(started, ["event-1"]);
  assert.deepEqual(persisted, ["event-2"]);
  assert.equal(queue.snapshot().queuedEvents, 1);
  queue.persistWaiting(item => persisted.push(item.id));
  release.resolve();
  await queue.waitForIdle();
  assert.deepEqual(started, ["event-1"]);
  assert.deepEqual(persisted, ["event-2", "event-3"]);
  assert.equal(queue.snapshot().pendingEvents, 0);
});
