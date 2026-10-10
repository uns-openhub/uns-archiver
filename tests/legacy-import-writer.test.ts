import assert from "node:assert/strict";
import test from "node:test";
import {
  writeLegacyEvent,
  parseLegacyPacket,
} from "../src/legacy-import-writer.js";
import { NonRetryableError } from "../src/errors.js";

const packet = {
  version: "2.0.0",
  message: { data: { value: 12, time: "2026-07-19T12:00:00.000Z" } },
};
const event = {
  topic: "plant/retired/motor/speed",
  message: JSON.stringify(packet),
};

test("legacy writer preserves historical timestamp without consulting current active topics", async () => {
  let received: any;
  const result = await writeLegacyEvent(event, {
    findStorage: () => ({ ingestMode: "append" }),
    write: async (value) => {
      received = value;
    },
  });
  assert.equal(result.outcome, "written");
  assert.equal(received.message.data.time, packet.message.data.time);
});

test("legacy 1.x table arrays normalize without changing time or grouping", async () => {
  const parsed = await parseLegacyPacket(
    JSON.stringify({
      version: "1.0.0",
      interval: 5,
      message: {
        table: {
          time: packet.message.data.time,
          dataGroup: "batch",
          columns: [
            { name: "state", type: "string", value: "OLD" },
            { name: "power", type: "double", value: 42, uom: "kW" },
          ],
        },
      },
    }),
  );
  assert.equal(parsed.version, "1.0.0");
  assert.equal(parsed.interval, 5);
  assert.equal(parsed.message.table?.time, packet.message.data.time);
  assert.equal(parsed.message.table?.dataGroup, "batch");
  assert.deepEqual(parsed.message.table?.columns, {
    state: { type: "string", value: "OLD" },
    power: { type: "double", value: 42, uom: "kW" },
  });
});
test("buffer envelopes and per-kind storage mode are resolved before writing", async () => {
  const parsed = await parseLegacyPacket(
    Buffer.from(JSON.stringify(packet)).toJSON(),
  );
  assert.equal(parsed.message.data?.value, 12);
  assert.deepEqual(
    await writeLegacyEvent(event, {
      findStorage: () => ({ ingestMode: "append" }),
      getMode: () => "window_replace",
      write: async () => assert.fail("must not write"),
    }),
    { outcome: "deferred", reason: "window-replace-requires-review" },
  );
});
test("invalid envelope, timestamp and no storage rule do not acknowledge a DB write", async () => {
  let writes = 0;
  const deps = {
    findStorage: () => ({}),
    write: async () => {
      writes++;
    },
  };
  assert.equal(
    (await writeLegacyEvent({ ...event, message: "invalid" }, deps)).outcome,
    "quarantined",
  );
  assert.equal(
    (
      await writeLegacyEvent(
        {
          ...event,
          message: JSON.stringify({
            ...packet,
            message: { data: { value: 1 } },
          }),
        },
        deps,
      )
    ).outcome,
    "quarantined",
  );
  assert.equal(
    (await writeLegacyEvent(event, { ...deps, findStorage: () => null }))
      .outcome,
    "quarantined",
  );
  assert.equal(writes, 0);
});
test("historical window replacement is deferred for review", async () => {
  assert.deepEqual(
    await writeLegacyEvent(event, {
      findStorage: () => ({ ingestMode: "window_replace" }),
      write: async () => {
        assert.fail("must not write");
      },
    }),
    { outcome: "deferred", reason: "window-replace-requires-review" },
  );
});
test("writer failure is deferred, non-retryable row is preserved for review", async () => {
  for (const [error, outcome] of [
    [new Error("sensitive connection details"), "deferred"],
    [new NonRetryableError("sensitive row"), "quarantined"],
  ] as const) {
    const result = await writeLegacyEvent(event, {
      findStorage: () => ({}),
      write: async () => {
        throw error;
      },
    });
    assert.equal(result.outcome, outcome);
    assert.ok(!JSON.stringify(result).includes("sensitive"));
  }
});
