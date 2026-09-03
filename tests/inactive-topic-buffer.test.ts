import assert from "node:assert/strict";
import test from "node:test";
import {
  discardExpiredOrOverflowInactiveEvents,
  isInactiveBufferSpill,
} from "../src/inactive-topic-buffer.js";

type Event = { id: string; receivedAt: number };

test("drops inactive packets after the metadata grace period", () => {
  const eventsByTopic = new Map<string, Event[]>([
    [
      "system/telemetry",
      [
        { id: "expired", receivedAt: 1_000 },
        { id: "recent", receivedAt: 4_500 },
      ],
    ],
  ]);

  const discarded = discardExpiredOrOverflowInactiveEvents(
    eventsByTopic,
    10,
    2_000,
    5_000,
  );

  assert.deepEqual(
    discarded.map((event) => event.id),
    ["expired"],
  );
  assert.deepEqual(
    eventsByTopic.get("system/telemetry")?.map((event) => event.id),
    ["recent"],
  );
});

test("drops the global oldest inactive packets instead of making them durable", () => {
  const eventsByTopic = new Map<string, Event[]>([
    ["a", [{ id: "oldest", receivedAt: 1_000 }]],
    ["b", [{ id: "middle", receivedAt: 2_000 }]],
    ["c", [{ id: "newest", receivedAt: 3_000 }]],
  ]);

  const discarded = discardExpiredOrOverflowInactiveEvents(
    eventsByTopic,
    2,
    10_000,
    3_000,
  );

  assert.deepEqual(
    discarded.map((event) => event.id),
    ["oldest"],
  );
  assert.equal(eventsByTopic.has("a"), false);
});

test("identifies only legacy inactive-buffer spool entries for acknowledgement", () => {
  assert.equal(
    isInactiveBufferSpill({ bufferReason: "inactive_expired" }),
    true,
  );
  assert.equal(
    isInactiveBufferSpill({ bufferReason: "inactive_overflow" }),
    true,
  );
  assert.equal(isInactiveBufferSpill({ bufferReason: "ingest_events" }), false);
  assert.equal(isInactiveBufferSpill({}), false);
});
