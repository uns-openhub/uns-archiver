import assert from "node:assert/strict";
import test from "node:test";
import { resolveEventDeduplicationDisposition } from "../src/event-deduplication.js";

test("keeps an inflight stored event durable until its original write is confirmed", () => {
  assert.equal(
    resolveEventDeduplicationDisposition(false, true, true),
    "defer-inflight",
  );
  assert.equal(
    resolveEventDeduplicationDisposition(true, true, true),
    "skip-confirmed",
  );
  assert.equal(
    resolveEventDeduplicationDisposition(false, true, false),
    "skip-confirmed",
  );
  assert.equal(
    resolveEventDeduplicationDisposition(false, false, true),
    "process",
  );
});
