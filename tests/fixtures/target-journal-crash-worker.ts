import path from "node:path";
import { TargetJournal } from "../../src/target-journal.js";
const root = process.argv[2];
const journal = new TargetJournal({
  directory: path.join(root, "journal"),
  policyFile: path.join(root, "policy.json"),
  reserveBytes: 0,
  retryMs: 1,
  write: async (entry) => {
    if (entry.id === "unknown-write") await new Promise<void>(() => {});
    throw new Error("Database unavailable");
  },
});
await journal.command(
  {
    action: "pause",
    requestId: "10000000-0000-4000-8000-000000000001",
    expectedRevision: 0,
    reason: "Crash test",
    selector: { tables: ["test_paused_data"] },
  },
  "local-operator",
);
for (let index = 0; index < 20; index++)
  await journal.submit([
    {
      id: `occurrence-${index}`,
      table: index % 2 ? "test_paused_data" : "test_other_data",
      topic: "canary/crash/value",
      payload: { value: 11 },
    },
  ]);
// Kill while an actual admitted writer has no response. The restart must retain
// uncertainty, not claim that the pause had a confirmed flush boundary.
void journal.submit([
  {
    id: "unknown-write",
    table: "test_uncertain_data",
    topic: "canary/crash/unknown",
    payload: { value: 12 },
  },
]);
await Promise.resolve();
await journal.command(
  {
    action: "pause",
    requestId: "10000000-0000-4000-8000-000000000002",
    expectedRevision: 1,
    reason: "Pause in-flight crash test",
    selector: { tables: ["test_uncertain_data"] },
  },
  "local-operator",
  2,
);
process.stdout.write("durably-accepted\n");
setInterval(() => {}, 1000);
