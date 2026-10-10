import assert from "node:assert/strict";
import test from "node:test";
import {
  createArchiverShutdown,
  drainArchiverForShutdown,
} from "../src/archiver-shutdown.js";

const deferred = () => {
  let resolve: () => void = () => undefined;
  const promise = new Promise<void>((nextResolve) => {
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
  assert.fail("Timed out while waiting for shutdown work.");
};

test("waits for active replay before closing the shared QuestDB writer", async () => {
  const live = deferred();
  const replay = deferred();
  const calls: string[] = [];
  const shutdown = drainArchiverForShutdown({
    stopMqtt: async () => {
      calls.push("mqtt");
    },
    waitForLiveIngest: async () => {
      calls.push("live");
      await live.promise;
    },
    waitForStoredReplay: async () => {
      calls.push("replay");
      await replay.promise;
    },
    closeQuestDb: async () => {
      calls.push("questdb");
    },
  });

  await Promise.resolve();
  assert.deepEqual(calls, ["mqtt", "live"]);
  live.resolve();
  await waitFor(() => calls.includes("replay"));
  assert.deepEqual(calls, ["mqtt", "live", "replay"]);
  replay.resolve();
  await shutdown;
  assert.deepEqual(calls, ["mqtt", "live", "replay", "questdb"]);
});

test("handover release stops new admission without interrupting accepted work", async () => {
  let released = 0;
  const calls: string[] = [];
  const live = deferred();
  const shutdown = createArchiverShutdown(
    () => {
      released += 1;
    },
    {
      stopMqtt: async () => {
        calls.push("mqtt");
      },
      waitForLiveIngest: async () => {
        calls.push("live");
        await live.promise;
      },
      waitForStoredReplay: async () => {
        calls.push("replay");
      },
      closeQuestDb: async () => {
        calls.push("writer");
      },
    },
  );
  shutdown.release();
  shutdown.release();
  assert.equal(released, 1);
  assert.deepEqual(calls, []);
  const handover = shutdown.drain();
  const signal = shutdown.drain();
  assert.equal(signal, handover);
  await waitFor(() => calls.includes("live"));
  assert.deepEqual(calls, ["mqtt", "live"]);
  live.resolve();
  await handover;
  assert.deepEqual(calls, ["mqtt", "live", "replay", "writer"]);
  await shutdown.drain();
  assert.equal(released, 1);
  assert.equal(calls.length, 4);
});

test("failed replay drain is shared with signal cleanup and never closes its pending writer", async () => {
  const failure = new Error("replay failed");
  let closes = 0;
  let releases = 0;
  const shutdown = createArchiverShutdown(
    () => {
      releases++;
    },
    {
      stopMqtt: async () => undefined,
      waitForLiveIngest: async () => undefined,
      waitForStoredReplay: async () => {
        throw failure;
      },
      closeQuestDb: async () => {
        closes++;
      },
    },
  );
  const first = shutdown.drain();
  assert.equal(shutdown.drain(), first);
  await assert.rejects(first, failure);
  await assert.rejects(shutdown.drain(), failure);
  assert.equal(closes, 0);
  assert.equal(releases, 1);
});
