import assert from "node:assert/strict";
import test from "node:test";
import { drainArchiverForShutdown } from "../src/archiver-shutdown.js";

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
