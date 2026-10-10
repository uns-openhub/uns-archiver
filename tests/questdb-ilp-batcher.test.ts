import assert from "node:assert/strict";
import test from "node:test";
import {
  QuestDbBatchCapacityError,
  QuestDbIlpBatcher,
} from "../src/writers/questdb-ilp-batcher.js";

const deferred = <T = void>() => {
  let resolve: (value: T | PromiseLike<T>) => void = () => undefined;
  let reject: (reason?: unknown) => void = () => undefined;
  const promise = new Promise<T>((nextResolve, nextReject) => {
    resolve = nextResolve;
    reject = nextReject;
  });
  return { promise, resolve, reject };
};

const createBatcher = (options?: {
  flush?: () => Promise<boolean | void>;
  flushIntervalMs?: number;
  maxRows?: number;
  maxPendingRows?: number;
}) => {
  let buffer: string[] = [];
  const flushed: string[][] = [];
  const batcher = new QuestDbIlpBatcher(
    {
      reset: () => {
        buffer = [];
      },
      flush: async () => {
        if (options?.flush) return await options.flush();
        flushed.push([...buffer]);
        return true;
      },
      shouldRetry: (error) => (error as { code?: string }).code === "ETIMEDOUT",
    },
    {
      flushIntervalMs: options?.flushIntervalMs ?? 20,
      maxRows: options?.maxRows ?? 10,
      maxPendingRows: options?.maxPendingRows ?? 20,
    },
  );
  return {
    batcher,
    flushed,
    write: (value: string) => async () => {
      buffer.push(value);
    },
  };
};

test("flushes rows that arrive within one interval as a single ILP batch", async () => {
  const { batcher, flushed, write } = createBatcher();

  await Promise.all([
    batcher.enqueue(write("first")),
    batcher.enqueue(write("second")),
    batcher.enqueue(write("third")),
  ]);

  assert.deepEqual(flushed, [["first", "second", "third"]]);
  assert.equal(batcher.snapshot().successfulFlushes, 1);
  assert.equal(batcher.snapshot().lastBatchRows, 3);
  assert.notEqual(batcher.snapshot().lastFlushDurationMs, null);
});

test("flushes immediately when maxRows is reached", async () => {
  const { batcher, flushed, write } = createBatcher({
    flushIntervalMs: 10_000,
    maxRows: 2,
  });

  await Promise.all([
    batcher.enqueue(write("one")),
    batcher.enqueue(write("two")),
  ]);

  assert.deepEqual(flushed, [["one", "two"]]);
  assert.equal(batcher.snapshot().lastBatchRows, 2);
});

test("flushes a partial batch after the configured timer", async () => {
  const { batcher, flushed, write } = createBatcher({
    flushIntervalMs: 15,
    maxRows: 10,
  });

  await batcher.enqueue(write("timer-row"));

  assert.deepEqual(flushed, [["timer-row"]]);
});

test("rebuilds and retries a failed flush before resolving every row promise", async () => {
  let buffer: string[] = [];
  const flushed: string[][] = [];
  let attempts = 0;
  const batcher = new QuestDbIlpBatcher(
    {
      reset: () => {
        buffer = [];
      },
      flush: async () => {
        attempts += 1;
        if (attempts === 1) {
          const error = new Error("temporary timeout") as Error & {
            code: string;
          };
          error.code = "ETIMEDOUT";
          throw error;
        }
        flushed.push([...buffer]);
        return true;
      },
      shouldRetry: (error) => (error as { code?: string }).code === "ETIMEDOUT",
    },
    { flushIntervalMs: 10_000, maxRows: 2, maxPendingRows: 4 },
  );

  const write = (value: string) => async () => {
    buffer.push(value);
  };
  await Promise.all([
    batcher.enqueue(write("first")),
    batcher.enqueue(write("second")),
  ]);

  assert.equal(attempts, 2);
  assert.deepEqual(flushed, [["first", "second"]]);
  assert.equal(batcher.snapshot().retriedFlushes, 1);
});

test("rejects every row promise when a batch flush fails permanently", async () => {
  const { batcher, write } = createBatcher({
    flushIntervalMs: 10_000,
    maxRows: 2,
    flush: async () => {
      throw new Error("QuestDB rejected this batch");
    },
  });

  const outcomes = await Promise.allSettled([
    batcher.enqueue(write("first")),
    batcher.enqueue(write("second")),
  ]);

  assert.deepEqual(
    outcomes.map((outcome) => outcome.status),
    ["rejected", "rejected"],
  );
  assert.equal(batcher.snapshot().failedFlushes, 1);
  assert.equal(batcher.snapshot().rejectedRows, 2);
});

test("close flushes the final partial batch before returning", async () => {
  const { batcher, flushed, write } = createBatcher({
    flushIntervalMs: 10_000,
  });
  const row = batcher.enqueue(write("shutdown-row"));

  await batcher.close();
  await row;

  assert.deepEqual(flushed, [["shutdown-row"]]);
});

test("enforces bounded pending rows while an earlier batch is flushing", async () => {
  const firstFlush = deferred<boolean>();
  const { batcher, write } = createBatcher({
    flushIntervalMs: 10_000,
    maxRows: 1,
    maxPendingRows: 2,
    flush: async () => await firstFlush.promise,
  });

  const first = batcher.enqueue(write("first"));
  const second = batcher.enqueue(write("second"));
  await assert.rejects(
    batcher.enqueue(write("overflow")),
    QuestDbBatchCapacityError,
  );

  firstFlush.resolve(true);
  await Promise.all([first, second]);
  assert.equal(batcher.snapshot().queuedRows, 0);
  assert.equal(batcher.snapshot().flushingRows, 0);
});

test("a replay wait hint flushes a shared partial batch without changing live defaults", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const { batcher, flushed, write } = createBatcher({
    flushIntervalMs: 1000,
    maxRows: 512,
    maxPendingRows: 2048,
  });
  const live = batcher.enqueue(write("live"));
  const replay = batcher.enqueue(write("replay"), 25);
  assert.equal(flushed.length, 0);
  t.mock.timers.tick(26);
  await Promise.all([live, replay]);
  assert.deepEqual(flushed, [["live", "replay"]]);
  assert.equal(batcher.snapshot().config.flushIntervalMs, 1000);
  assert.equal(batcher.snapshot().queueWait.rows, 2);
  assert.ok(batcher.snapshot().queueWait.totalMs >= 0);
  await batcher.close();
});

test("a replay hint never postpones an earlier normal deadline", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const { batcher, flushed, write } = createBatcher({
    flushIntervalMs: 10,
    maxRows: 512,
  });
  const live = batcher.enqueue(write("live"));
  const replay = batcher.enqueue(write("replay"), 25);
  t.mock.timers.tick(11);
  await Promise.all([live, replay]);
  assert.deepEqual(flushed, [["live", "replay"]]);
  await batcher.close();
});

test("a due replay batch stays serialized behind an in-flight flush and needs its own ACK", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const firstAck = deferred();
  let flushes = 0,
    active = 0,
    maximum = 0;
  const { batcher, write } = createBatcher({
    maxRows: 2,
    flushIntervalMs: 1000,
    flush: async () => {
      active++;
      maximum = Math.max(maximum, active);
      if (++flushes === 1) await firstAck.promise;
      active--;
      return true;
    },
  });
  const one = batcher.enqueue(write("one")),
    two = batcher.enqueue(write("two"));
  let replayDone = false;
  const replay = batcher.enqueue(write("replay"), 25).then(() => {
    replayDone = true;
  });
  t.mock.timers.tick(26);
  assert.equal(replayDone, false);
  firstAck.resolve();
  await Promise.all([one, two]);
  t.mock.timers.tick(26);
  await replay;
  assert.equal(flushes, 2);
  assert.equal(maximum, 1);
  await batcher.close();
});

test("invalid wait hints admit no row", async () => {
  const { batcher, write } = createBatcher();
  for (const hint of [0, -1, NaN, Infinity])
    await assert.rejects(
      batcher.enqueue(write("bad"), hint),
      /Invalid QuestDB batch wait hint/,
    );
  assert.equal(batcher.snapshot().queuedRows, 0);
  await batcher.close();
});
