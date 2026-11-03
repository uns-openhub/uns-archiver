import assert from "node:assert/strict";
import test from "node:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { projectExtrasSchema } from "../src/config/project.config.extension.js";
import { TargetJournal, type JournalEntry } from "../src/target-journal.js";

const proposal = { maxBytes: 34359738368, reserveBytes: 2147483648, maxPendingPerTarget: 10000000 };
test("a larger bounded maintenance budget is schema-valid; the ten-million target ceiling stays enforced", () => {
  const config = (limits: unknown) => ({ archiver: { targetJournal: limits }, questdb: { configurationString: "http::addr=localhost:9000;", dataStorage: [{ topic: "example/#", tablePrefix: "sample" }] } });
  assert.equal(projectExtrasSchema.safeParse(config(proposal)).success, true);
  assert.equal(projectExtrasSchema.safeParse(config({ ...proposal, maxPendingPerTarget: 10000001 })).success, false);
  assert.equal(projectExtrasSchema.safeParse(config({ ...proposal, maxBytes: 1099511627777 })).success, false);
});
test("budget increase takes effect on reopen without losing accepted parts, pause policy or command receipts", async () => {
  const root = mkdtempSync(path.join(tmpdir(), "archive-capacity-reopen-"));
  const rows: JournalEntry<{ value: number }>[] = [];
  const options = { directory: path.join(root, "journal"), policyFile: path.join(root, "policy.json"), reserveBytes: 0,
    write: async (entry: JournalEntry<{ value: number }>) => { rows.push(entry); } };
  let journal = new TargetJournal({ ...options, maxBytes: 1048576, maxPendingPerTarget: 2 });
  const pause = { action: "pause", requestId: randomUUID(), expectedRevision: 0, reason: "Local capacity acceptance", selector: { tables: ["example_data"] } };
  const event = (value: number): JournalEntry<{ value: number }> => ({ id: randomUUID(), table: "example_data", topic: "canary/capacity/reading", payload: { value } });
  const retained = [event(1), event(2)]; const retry = event(3);
  try {
    await journal.command(pause, "synthetic-operator"); await journal.submit(retained);
    await assert.rejects(journal.submit([retry]));
    assert.equal(journal.snapshot().targets[0].queued, 2);
    await journal.close();
    // The production reserve is schema-checked above. A zero test-only reserve
    // isolates reopen semantics from the developer machine's small free disk.
    journal = new TargetJournal({ ...options, maxBytes: proposal.maxBytes, maxPendingPerTarget: proposal.maxPendingPerTarget });
    assert.equal(journal.snapshot().diskBudgetBytes, proposal.maxBytes);
    assert.equal(journal.snapshot().payloadBudgetBytes, proposal.maxBytes / 2);
    assert.equal(journal.snapshot().maxPendingPerTarget, proposal.maxPendingPerTarget);
    assert.equal(journal.snapshot().reserveBytes, 0);
    assert.equal(journal.snapshot().targets[0].queued, 2);
    assert.equal(journal.snapshot().policy.revision, 1);
    assert.equal(journal.snapshot().boundary.phase, "paused");
    assert.equal((await journal.command(pause, "synthetic-operator")).revision, 1);
    await journal.submit([retry]); assert.equal(journal.snapshot().targets[0].queued, 3);
    await journal.command({ action: "resume", requestId: randomUUID(), pauseId: pause.requestId, expectedRevision: 1, reason: "Local capacity acceptance finished" }, "synthetic-operator");
    await journal.replay();
    assert.deepEqual(rows.map(row => row.id), [...retained, retry].map(row => row.id));
    assert.equal(journal.snapshot().targets[0].queued, 0);
  } finally { await journal.close(); rmSync(root, { recursive: true, force: true }); }
});
