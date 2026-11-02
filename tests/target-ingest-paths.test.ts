import assert from "node:assert/strict";
import test from "node:test";
import {
  mkdtempSync,
  mkdirSync,
  readFileSync,
  realpathSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import { randomUUID } from "node:crypto";
import type { Sender } from "@questdb/nodejs-client";
import {
  QuestDBWriter,
  resolvePacketTargets,
} from "../src/writers/questDbWriter.js";
import { LegacyImportManager } from "../src/legacy-import.js";
import { writeLegacyEvent } from "../src/legacy-import-writer.js";
import { StoredEventReplay } from "../src/stored-event-replay.js";
import { persistEventFile } from "../src/durable-event-file.js";

// Row-level sink: assertions inspect final table names, source IDs and values, not HTTP counts.
class RowSender {
  rows: any[] = [];
  private pending: any[] = [];
  private current: any;
  table(name: string) {
    this.current = { table: name };
    return this;
  }
  symbol(name: string, value: unknown) {
    this.current[name] = value;
    return this;
  }
  stringColumn(name: string, value: unknown) {
    this.current[name] = value;
    return this;
  }
  floatColumn(name: string, value: unknown) {
    this.current[name] = value;
    return this;
  }
  intColumn(name: string, value: unknown) {
    this.current[name] = value;
    return this;
  }
  booleanColumn(name: string, value: unknown) {
    this.current[name] = value;
    return this;
  }
  timestampColumn(name: string, value: unknown) {
    this.current[name] = value;
    return this;
  }
  async at(time: number) {
    this.pending.push({ ...this.current, time });
  }
  async flush() {
    this.rows.push(...this.pending);
    this.pending = [];
  }
  reset() {
    this.pending = [];
    return this;
  }
  async close() {}
}
const packet = {
  version: "2.0.0",
  message: {
    data: {
      time: "2026-11-02T12:00:00.000Z",
      value: 17,
      uom: "kWh",
      dataGroup: "power",
    },
    table: {
      time: "2026-11-02T12:00:00.000Z",
      columns: { reading: { type: "double", value: 19, uom: "kWh" } },
      dataGroup: "status",
    },
  },
} as any;

test("final destinations split data/table groups through the actual writer", async () => {
  assert.deepEqual(
    resolvePacketTargets(packet, "test").map((target) => target.table),
    ["test_power_data", "test_status_table"],
  );
  const root = mkdtempSync(path.join(tmpdir(), "archiver-target-split-"));
  const sender = new RowSender();
  const writer = new QuestDBWriter(sender as unknown as Sender, undefined, {
    maxRows: 1,
  });
  const journal = writer.configureTargetJournal({
    directory: path.join(root, "journal"),
    policyFile: path.join(root, "policy.json"),
    reserveBytes: 0,
  });
  try {
    const pauseId = randomUUID();
    await journal.command(
      {
        action: "pause",
        requestId: pauseId,
        expectedRevision: 0,
        reason: "Test",
        selector: { tables: ["test_power_data"] },
      },
      "operator",
    );
    await writer.writeUnsPacket(
      packet,
      "test",
      "canary/object/reading",
      undefined,
      undefined,
      undefined,
      { occurrenceId: "mixed-occurrence" },
    );
    assert.deepEqual(
      sender.rows.map((row) => [row.table, row.reading]),
      [["test_status_table", 19]],
    );
    assert.equal(journal.snapshot().targets.find((t) => t.paused)?.queued, 1);
    await journal.command(
      {
        action: "resume",
        requestId: randomUUID(),
        pauseId,
        expectedRevision: 1,
        reason: "Test done",
      },
      "operator",
    );
    await journal.replay();
    assert.deepEqual(
      sender.rows.map((row) => row.table),
      ["test_status_table", "test_power_data"],
    );
    assert.equal(sender.rows[1].numberValue, 17);
  } finally {
    await journal.close();
    await writer.close();
    rmSync(root, { recursive: true, force: true });
  }
});

test("legacy files remain intact during pause; healthy mixed parts do not repeat on retry", async () => {
  const root = realpathSync(
    mkdtempSync(path.join(tmpdir(), "archiver-target-legacy-")),
  );
  const source = path.join(root, "old");
  const live = path.join(root, "live");
  mkdirSync(source);
  mkdirSync(live);
  const sender = new RowSender();
  const writer = new QuestDBWriter(sender as unknown as Sender, undefined, {
    maxRows: 1,
  });
  const journal = writer.configureTargetJournal({
    directory: path.join(root, "journal"),
    policyFile: path.join(root, "policy.json"),
    reserveBytes: 0,
  });
  const manager = new LegacyImportManager({
    sources: [{ id: "test-source", directory: source }],
    liveDirectory: live,
    instanceId: "test-local",
    policyDigest: () => "test-policy",
    canWrite: () => true,
    hasLiveHeadroom: () => true,
    write: (event, context) =>
      writeLegacyEvent(event, {
        findStorage: () => ({ ingestMode: "append" }),
        write: async (packet) => {
          await writer.writeUnsPacket(
            packet,
            "test",
            event.topic,
            undefined,
            undefined,
            undefined,
            {
              maxBatchWaitMs: 1,
              occurrenceId: context.eventKey,
              legacyKey: context.eventKey,
            },
          );
        },
      }),
    acknowledged: (key) => journal.acknowledgeLegacy(key),
  });
  try {
    const pauseId = randomUUID();
    await journal.command(
      {
        action: "pause",
        requestId: pauseId,
        expectedRevision: 0,
        reason: "Test",
        selector: { tables: ["test_power_data"] },
      },
      "operator",
    );
    const text = JSON.stringify({
      topic: "canary/legacy/reading",
      message: packet,
    });
    for (const name of ["one", "two"])
      writeFileSync(path.join(source, `${name}.event`), text);
    await manager.command({
      action: "start",
      sourceId: "test-source",
      requestId: "legacy-start",
      expectedRevision: 0,
      confirmSourceClosed: true,
    });
    await manager.tick();
    assert.equal(readFileSync(path.join(source, "one.event"), "utf8"), text);
    assert.equal(readFileSync(path.join(source, "two.event"), "utf8"), text);
    assert.equal(
      sender.rows.filter((row) => row.table === "test_status_table").length,
      2,
    );
    await journal.command(
      {
        action: "resume",
        requestId: randomUUID(),
        pauseId,
        expectedRevision: 1,
        reason: "Test done",
      },
      "operator",
    );
    await journal.replay();
    await journal.replay();
    await new Promise((resolve) => setTimeout(resolve, 5100));
    for (let n = 0; n < 3; n++) await manager.tick();
    assert.deepEqual(
      readdirSync(source).filter((name) => name.endsWith(".event")),
      [],
    );
    assert.equal(
      sender.rows.filter((row) => row.table === "test_power_data").length,
      2,
    );
    assert.equal(
      sender.rows.filter((row) => row.table === "test_status_table").length,
      2,
    );
  } finally {
    await manager.close();
    await journal.close();
    await writer.close();
    rmSync(root, { recursive: true, force: true });
  }
});

test("stored replay moves a paused occurrence only after target journal durability, then resumes it", async () => {
  const root = mkdtempSync(path.join(tmpdir(), "archiver-target-stored-"));
  const events = path.join(root, "events"),
    failed = path.join(events, "failed");
  mkdirSync(failed, { recursive: true });
  const sender = new RowSender();
  const writer = new QuestDBWriter(sender as unknown as Sender, undefined, {
    maxRows: 1,
  });
  const journal = writer.configureTargetJournal({
    directory: path.join(root, "journal"),
    policyFile: path.join(root, "policy.json"),
    reserveBytes: 0,
  });
  const replay = new StoredEventReplay({
    eventStorageDirectory: events,
    failedStorageDirectory: failed,
    eventFileExtension: ".event",
    processingExtension: ".processing",
    isReady: () => true,
    isStopping: () => false,
    hasLiveHeadroom: () => true,
    getLimits: () => ({ batchSize: 8, concurrency: 2 }),
    processEvent: async (event: any) => {
      await writer.writeUnsPacket(
        event.message,
        "test",
        event.topic,
        undefined,
        undefined,
        undefined,
        { occurrenceId: event.id },
      );
      return true;
    },
  });
  try {
    const pauseId = randomUUID();
    await journal.command(
      {
        action: "pause",
        requestId: pauseId,
        expectedRevision: 0,
        reason: "Test",
        selector: { tables: ["test_power_data"] },
      },
      "operator",
    );
    const onlyData = { ...packet, message: { data: packet.message.data } };
    persistEventFile(
      events,
      { id: "stored-1", topic: "canary/stored/reading", message: onlyData },
      "stored-1",
    );
    for (let n = 0; n < 4; n++) await replay.run();
    assert.deepEqual(
      readdirSync(events).filter(
        (name) => name.endsWith(".event") || name.endsWith(".processing"),
      ),
      [],
    );
    assert.equal(journal.snapshot().targets[0].queued, 1);
    assert.equal(sender.rows.length, 0);
    await journal.command(
      {
        action: "resume",
        requestId: randomUUID(),
        pauseId,
        expectedRevision: 1,
        reason: "Test done",
      },
      "operator",
    );
    await journal.replay();
    assert.equal(sender.rows.length, 1);
    assert.equal(sender.rows[0].numberValue, 17);
  } finally {
    await replay.close();
    await journal.close();
    await writer.close();
    rmSync(root, { recursive: true, force: true });
  }
});
