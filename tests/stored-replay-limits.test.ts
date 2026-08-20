import assert from "node:assert/strict";
import test from "node:test";
import {
  hasStoredReplayLiveHeadroom,
  resolveStoredReplayLimits,
} from "../src/stored-replay-limits.js";

test("derives a bounded replay batch and concurrency from live capacity", () => {
  assert.deepEqual(resolveStoredReplayLimits(512, 64), {
    batchSize: 64,
    concurrency: 64,
  });
  assert.deepEqual(resolveStoredReplayLimits(256, 64), {
    batchSize: 64,
    concurrency: 32,
  });
  assert.deepEqual(resolveStoredReplayLimits(8, 64), {
    batchSize: 4,
    concurrency: 1,
  });
});

test("reserves one quarter of live event and byte capacity for MQTT", () => {
  assert.equal(
    hasStoredReplayLiveHeadroom(
      { pendingEvents: 79, pendingBytes: 1_000, spillingEvents: 0 },
      512,
      16 * 1024 * 1024,
    ),
    true,
  );
  assert.equal(
    hasStoredReplayLiveHeadroom(
      { pendingEvents: 384, pendingBytes: 1_000, spillingEvents: 0 },
      512,
      16 * 1024 * 1024,
    ),
    false,
  );
  assert.equal(
    hasStoredReplayLiveHeadroom(
      { pendingEvents: 1, pendingBytes: 12 * 1024 * 1024, spillingEvents: 0 },
      512,
      16 * 1024 * 1024,
    ),
    false,
  );
  assert.equal(
    hasStoredReplayLiveHeadroom(
      { pendingEvents: 1, pendingBytes: 1_000, spillingEvents: 1 },
      512,
      16 * 1024 * 1024,
    ),
    false,
  );
});
