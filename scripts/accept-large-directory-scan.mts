import assert from "node:assert/strict";
import { setImmediate as yieldLoop } from "node:timers/promises";
import { performance } from "node:perf_hooks";
import { promises as fs, type Dirent } from "node:fs";
import {
  BoundedDirectoryScan,
  type DirectoryCursor,
} from "../src/bounded-directory-scan.js";

// A lazy directory oracle proves scanner work/memory bounds without creating
// millions of files. It does not measure filesystem latency or importer throughput.
const count = Number(process.argv[2] ?? 50_000_000);
assert.ok(
  Number.isSafeInteger(count) && count >= 100_000 && count <= 100_000_000,
);
const output = process.argv[3];
function cursor(total: number) {
  let read = 0,
    closed = false;
  const value: DirectoryCursor = {
    async read() {
      assert.ok(!closed);
      if (read === total) return null;
      return { name: `${read++}.event`, isFile: () => true } as Dirent;
    },
    async close() {
      closed = true;
    },
  };
  return {
    value,
    get read() {
      return read;
    },
    get closed() {
      return closed;
    },
  };
}
const early = cursor(count);
const stoppable = new BoundedDirectoryScan("lazy", async () => early.value);
let visits = 0;
await stoppable.pass(() => {
  visits++;
});
assert.ok(visits > 0 && visits <= 512);
await stoppable.close();
assert.ok(early.closed);
assert.equal(
  (
    await stoppable.pass(() => {
      throw Error("closed scanner ran");
    })
  ).entriesVisited,
  0,
);

const all = cursor(count);
const scan = new BoundedDirectoryScan("lazy", async () => all.value);
let total = 0,
  passes = 0,
  maximumEntriesPerPass = 0,
  maximumPassMs = 0;
let maximumSampledRssBytes = process.memoryUsage().rss;
const started = performance.now();
for (;;) {
  const before = performance.now();
  const pass = await scan.pass(() => {
    total++;
  });
  maximumPassMs = Math.max(maximumPassMs, performance.now() - before);
  maximumEntriesPerPass = Math.max(maximumEntriesPerPass, pass.entriesVisited);
  assert.ok(pass.entriesVisited <= 512);
  passes++;
  if (passes % 1000 === 0)
    maximumSampledRssBytes = Math.max(
      maximumSampledRssBytes,
      process.memoryUsage().rss,
    );
  if (pass.complete) break;
  await yieldLoop();
}
await scan.close();
assert.equal(total, count);
assert.ok(all.closed);
const evidence = {
  scope: "lazy-directory-scanner-only",
  logicalEntries: count,
  excludes: [
    "physical-filesystem",
    "legacy-import-throughput",
    "QuestDB",
    "50-million-real-files-acceptance",
  ],
  passes,
  maximumEntriesPerPass,
  maximumPassMs,
  maximumSampledRssBytes,
  elapsedMs: performance.now() - started,
  earlyCloseEntriesVisited: visits,
  allEntriesVisited: total,
};
if (output) await fs.writeFile(output, JSON.stringify(evidence, null, 2));
console.log(JSON.stringify(evidence));
