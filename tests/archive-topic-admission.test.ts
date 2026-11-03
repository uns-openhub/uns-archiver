import assert from "node:assert/strict";
import test from "node:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { ArchiveTopicAdmission } from "../src/archive-topic-admission.js";
import { projectExtrasSchema } from "../src/config/project.config.extension.js";
import { TargetJournal, type JournalEntry } from "../src/target-journal.js";
const questdb = (excludeTopics?: unknown) => ({
  configurationString: "http::addr=localhost:9000;",
  dataStorage: [{ topic: "#", tablePrefix: "test" }],
  ...(excludeTopics === undefined ? {} : { excludeTopics }),
});
test("optional exclusions preserve old configs and validate ordinary MQTT filters", () => {
  for (const filters of [
    undefined,
    [],
    ["factory/excluded/#", "example/+/reading", "one/exact/topic"],
  ]) {
    assert.ok(
      projectExtrasSchema.safeParse({ questdb: questdb(filters) }).success,
    );
  }
  for (const filters of [
    ["!/factory/excluded"],
    ["factory/excluded/#/data"],
    ["factory/blo+cked/#"],
    [""],
    ["bad\u0000topic"],
    Array(129).fill("example/#"),
  ]) {
    assert.equal(
      projectExtrasSchema.safeParse({ questdb: questdb(filters) }).success,
      false,
    );
  }
});
test("exclusions override broad includes, respect subtree/exact/+ boundaries, and reload both ways", () => {
  const gate = new ArchiveTopicAdmission();
  assert.equal(gate.accepts("factory/excluded/a/reading"), true);
  gate.update(["factory/excluded/#", "example/+/reading", "single/exact"]);
  for (const topic of [
    "factory/excluded",
    "factory/excluded/a/reading",
    "example/line/reading",
    "single/exact",
  ])
    assert.equal(gate.accepts(topic), false);
  for (const topic of [
    "factory/excluded-neighbour/a/reading",
    "factory/allowed/a/reading",
    "example/line/more/reading",
    "single/exact/child",
  ])
    assert.equal(gate.accepts(topic), true);
  assert.equal(gate.snapshot().excludedEvents, 4);
  for (const invalid of ["factory/excluded/#", ["!/factory/excluded"], ["invalid/#/child"]]) {
    assert.throws(() => gate.update(invalid));
    assert.equal(gate.snapshot().excludeTopics[0], "factory/excluded/#");
  }
  gate.update([]);
  assert.equal(gate.accepts("factory/excluded/a/reading"), true);
});
test("new excluded events never enter a queue; already accepted records remain replayable after policy change/reopen", async () => {
  const root = mkdtempSync(path.join(tmpdir(), "archive-exclusion-"));
  const rows: JournalEntry<{ value: number }>[] = [];
  let down = true;
  const options = {
    directory: path.join(root, "journal"),
    policyFile: path.join(root, "policy.json"),
    reserveBytes: 0,
    retryMs: 1,
    write: async (entry: JournalEntry<{ value: number }>) => {
      if (down) throw Error("synthetic outage");
      rows.push(entry);
    },
  };
  let journal = new TargetJournal(options);
  const gate = new ArchiveTopicAdmission();
  const incoming = async (topic: string, value: number) => {
    if (gate.accepts(topic))
      await journal.submit([
        {
          id: randomUUID(),
          table: "test_input_data",
          topic,
          payload: { value },
        },
      ]);
  };
  try {
    await incoming("factory/excluded/old/reading", 1); // Accepted before the config change.
    gate.update(["factory/excluded/#"]);
    await incoming("factory/excluded/new/reading", 2);
    await incoming("factory/allowed/line/reading", 3);
    assert.equal(
      journal
        .snapshot()
        .targets.reduce((n, target) => n + Number(target.queued), 0),
      2,
    );
    await journal.close();
    journal = new TargetJournal(options);
    down = false;
    for (let i = 0; i < 6; i++) {
      await new Promise((resolve) => setTimeout(resolve, 5));
      await journal.replay();
    }
    assert.deepEqual(
      rows.map((row) => row.payload.value),
      [1, 3],
    );
    gate.update([]);
    await incoming("factory/excluded/new/reading", 4);
    assert.deepEqual(
      rows.map((row) => row.payload.value),
      [1, 3, 4],
    );
    assert.equal(
      journal
        .snapshot()
        .targets.reduce((n, target) => n + Number(target.queued), 0),
      0,
    );
  } finally {
    await journal.close();
    rmSync(root, { recursive: true, force: true });
  }
});
