import assert from "node:assert/strict";
import test from "node:test";
import type { Sender } from "@questdb/nodejs-client";
import { QuestDBWriter } from "../src/writers/questDbWriter.js";

type WriterCall = [method: string, name?: string, value?: unknown];

class FakeQuestDbSender {
  readonly calls: WriterCall[] = [];

  constructor(private readonly onFlush?: (count: number) => Promise<void>) {}

  table(name: string): this {
    this.calls.push(["table", name]);
    return this;
  }

  symbol(name: string, value: unknown): this {
    this.calls.push(["symbol", name, value]);
    return this;
  }

  booleanColumn(name: string, value: unknown): this {
    this.calls.push(["booleanColumn", name, value]);
    return this;
  }

  timestampColumn(name: string, value: unknown): this {
    this.calls.push(["timestampColumn", name, value]);
    return this;
  }

  stringColumn(name: string, value: unknown): this {
    this.calls.push(["stringColumn", name, value]);
    return this;
  }

  floatColumn(name: string, value: unknown): this {
    this.calls.push(["floatColumn", name, value]);
    return this;
  }

  intColumn(name: string, value: unknown): this {
    this.calls.push(["intColumn", name, value]);
    return this;
  }

  arrayColumn(name: string, value: unknown): this {
    this.calls.push(["arrayColumn", name, value]);
    return this;
  }

  async at(value: unknown): Promise<void> {
    this.calls.push(["at", undefined, value]);
  }

  async flush(): Promise<void> {
    this.calls.push(["flush"]);
    await this.onFlush?.(this.calls.filter(([method]) => method === "flush").length);
  }

  reset(): this {
    this.calls.push(["reset"]);
    return this;
  }

  async close(): Promise<void> {}
}

test("writes canonical object columns while preserving symbol and UoM behavior", async () => {
  const sender = new FakeQuestDbSender();
  const writer = new QuestDBWriter(sender as unknown as Sender, undefined, { maxRows: 1 });

  await writer.writeUnsPacket(
    {
      version: "2.0.0",
      message: {
        table: {
          time: "2026-07-19T12:00:00.000Z",
          columns: {
            state: { type: "symbol", value: "RUNNING" },
            power: { type: "double", value: 42.1, uom: "kW" },
            note: { type: "string", value: "stable" },
            optional: { type: "double", value: null },
          },
        },
      },
    } as never,
    "uns_measurements",
    "plant/line-1/equipment/main/measurements",
    undefined,
    undefined,
    {
      stableEntityId: "11111111-1111-4111-8111-111111111111",
      entityTypeKey: "openhub.asset",
      bindingRevision: "8",
      bindingDigest: `sha256:${"1".repeat(64)}`,
      resolution: "resolved",
      timeBasis: "source-event-time",
    },
  );

  const stateSymbolIndex = sender.calls.findIndex(
    ([method, name]) => method === "symbol" && name === "state",
  );
  const firstFieldIndex = sender.calls.findIndex(
    ([method]) => method === "booleanColumn",
  );

  assert.ok(stateSymbolIndex >= 0);
  assert.ok(firstFieldIndex > stateSymbolIndex, "symbol columns must be written before fields");
  assert.ok(
    sender.calls.some(
      ([method, name, value]) =>
        method === "floatColumn" && name === "power" && value === 42.1,
    ),
  );
  assert.ok(sender.calls.some(([method, name, value]) =>
    method === "symbol" && name === "stableEntityId" && value === "11111111-1111-4111-8111-111111111111"));
  assert.ok(sender.calls.some(([method, name, value]) =>
    method === "symbol" && name === "identityBindingRevision" && value === "8"));
  assert.ok(sender.calls.some(([method, name, value]) =>
    method === "stringColumn" && name === "identityBindingDigest" && value === `sha256:${"1".repeat(64)}`));
  assert.ok(sender.calls.some(([method, name, value]) =>
    method === "stringColumn" && name === "fullTopic" && value === "plant/line-1/equipment/main/measurements"));
  assert.ok(
    sender.calls.some(
      ([method, name, value]) =>
        method === "stringColumn" && name === "power_uom" && value === "kW",
    ),
  );
  assert.ok(
    sender.calls.some(
      ([method, name, value]) =>
        method === "stringColumn" && name === "note" && value === "stable",
    ),
  );
  assert.equal(sender.calls.some(([, name]) => name === "optional"), false);
  assert.equal(sender.calls.at(-1)?.[0], "flush");
});

test("batches data and table rows through one shared sender without interleaving row content", async () => {
  const sender = new FakeQuestDbSender();
  const writer = new QuestDBWriter(sender as unknown as Sender, undefined, {
    flushIntervalMs: 10_000,
    maxRows: 2,
    maxPendingRows: 4,
  });

  await Promise.all([
    writer.writeUnsPacket(
      {
        version: "2.0.0",
        message: {
          data: {
            time: "2026-07-19T12:00:00.000Z",
            value: 12.5,
            uom: "bar",
          },
        },
      } as never,
      "uns_measurements",
      "plant/line-1/equipment/main/pressure",
    ),
    writer.writeUnsPacket(
      {
        version: "2.0.0",
        message: {
          table: {
            time: "2026-07-19T12:00:01.000Z",
            columns: {
              state: { type: "symbol", value: "RUNNING" },
              power: { type: "double", value: 42.1, uom: "kW" },
            },
          },
        },
      } as never,
      "uns_measurements",
      "plant/line-1/equipment/main/measurements",
    ),
  ]);

  const tableIndexes = sender.calls
    .map(([method], index) => (method === "table" ? index : -1))
    .filter((index) => index >= 0);
  assert.equal(tableIndexes.length, 2);
  for (const [index, start] of tableIndexes.entries()) {
    const end = tableIndexes[index + 1] ?? sender.calls.length;
    assert.ok(
      sender.calls.slice(start, end).some(([method]) => method === "at"),
      "each table row must be complete before the next row starts",
    );
  }
  assert.equal(sender.calls.filter(([method]) => method === "flush").length, 1);
  assert.ok(sender.calls.some(([method, name, value]) => method === "floatColumn" && name === "value" && value === 12.5));
  assert.ok(sender.calls.some(([method, name, value]) => method === "stringColumn" && name === "uom" && value === "bar"));
  assert.ok(sender.calls.some(([method, name, value]) => method === "symbol" && name === "state" && value === "RUNNING"));
  assert.ok(sender.calls.some(([method, name, value]) => method === "floatColumn" && name === "power" && value === 42.1));
});

test("uses the shared batch limit before a single table can build an unbounded write chain", async () => {
  let resolveFirstFlush: (() => void) | undefined;
  const firstFlush = new Promise<void>((resolve) => {
    resolveFirstFlush = resolve;
  });
  const sender = new FakeQuestDbSender(async (count) => {
    if (count === 1) await firstFlush;
  });
  const writer = new QuestDBWriter(sender as unknown as Sender, undefined, {
    flushIntervalMs: 10_000,
    maxRows: 1,
    maxPendingRows: 2,
  });
  const packet = (time: string) =>
    ({
      version: "2.0.0",
      message: { data: { time, value: 1 } },
    }) as never;

  const first = writer.writeUnsPacket(packet("2026-07-19T12:00:00.000Z"), "uns_measurements", "plant/a/b/c/one");
  await new Promise((resolve) => setImmediate(resolve));
  const second = writer.writeUnsPacket(packet("2026-07-19T12:00:01.000Z"), "uns_measurements", "plant/a/b/c/two");
  const overflow = writer.writeUnsPacket(packet("2026-07-19T12:00:02.000Z"), "uns_measurements", "plant/a/b/c/three");

  await assert.rejects(overflow, /QuestDB ILP batch queue is full/);
  resolveFirstFlush?.();
  await Promise.all([first, second]);
});
