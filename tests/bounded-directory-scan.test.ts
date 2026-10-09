import assert from "node:assert/strict";
import type { Dirent } from "node:fs";
import test from "node:test";
import { BoundedDirectoryScan } from "../src/bounded-directory-scan.js";

const entry = (name: string) => ({ name, isFile: () => true }) as Dirent;
const fixture = (names: string[], now?: () => number) => {
  let opens = 0,
    closes = 0,
    reads = 0;
  const scanner = new BoundedDirectoryScan(
    "synthetic",
    async () => {
      opens++;
      let index = 0;
      return {
        read: async () => {
          reads++;
          return index < names.length ? entry(names[index++]) : null;
        },
        close: async () => {
          closes++;
        },
      };
    },
    now,
  );
  return { scanner, stats: () => ({ opens, closes, reads }) };
};
const limits = { maxEntries: 3, maxDurationMs: 1000 };

test("counts every entry including non-matches and retains one cursor across passes", async () => {
  const { scanner, stats } = fixture([
    "a.tmp",
    "b.processing",
    "c.tmp",
    "d.event",
    "e.tmp",
    "f.event",
    "g.tmp",
  ]);
  const seen: string[] = [];
  const visit = (item: Dirent) => {
    seen.push(item.name);
  };
  assert.deepEqual(await scanner.pass(visit, limits), {
    entriesVisited: 3,
    complete: false,
  });
  assert.deepEqual(await scanner.pass(visit, limits), {
    entriesVisited: 3,
    complete: false,
  });
  assert.deepEqual(await scanner.pass(visit, limits), {
    entriesVisited: 1,
    complete: true,
  });
  assert.deepEqual(seen, [
    "a.tmp",
    "b.processing",
    "c.tmp",
    "d.event",
    "e.tmp",
    "f.event",
    "g.tmp",
  ]);
  assert.deepEqual(stats(), { opens: 1, closes: 1, reads: 8 });
  await scanner.close();
});

test("time budget includes visitor work and resumes without another scan from the start", async () => {
  let clock = 0;
  const { scanner, stats } = fixture(["a", "b", "c"], () => clock);
  const seen: string[] = [];
  const visit = (item: Dirent) => {
    seen.push(item.name);
    clock += 10;
  };
  assert.deepEqual(
    await scanner.pass(visit, { maxEntries: 100, maxDurationMs: 15 }),
    { entriesVisited: 2, complete: false },
  );
  assert.deepEqual(
    await scanner.pass(visit, { maxEntries: 100, maxDurationMs: 15 }),
    { entriesVisited: 1, complete: true },
  );
  assert.deepEqual(seen, ["a", "b", "c"]);
  assert.equal(stats().opens, 1);
  await scanner.close();
});

test("stopping on a selected batch leaves later entries for the next pass", async () => {
  const { scanner } = fixture(["a.event", "b.event", "c.event"]);
  const seen: string[] = [];
  const visit = (item: Dirent) => {
    seen.push(item.name);
    return false;
  };
  await scanner.pass(visit, limits);
  await scanner.pass(visit, limits);
  await scanner.pass(visit, limits);
  assert.deepEqual(seen, ["a.event", "b.event", "c.event"]);
  await scanner.close();
});

test("a stopped scan does not open storage", async () => {
  const { scanner, stats } = fixture(["a"]);
  assert.deepEqual(
    await scanner.pass(
      () => {},
      limits,
      () => true,
    ),
    { entriesVisited: 0, complete: false },
  );
  assert.equal(stats().opens, 0);
  await scanner.close();
});

test("coalesces overlapping passes and close waits for a pending filesystem read", async () => {
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  let reads = 0,
    closes = 0;
  const scanner = new BoundedDirectoryScan("synthetic", async () => ({
    read: async () => {
      reads++;
      await gate;
      return entry("a");
    },
    close: async () => {
      closes++;
    },
  }));
  const first = scanner.pass(() => false, limits);
  const overlapping = scanner.pass(() => false, limits);
  assert.equal(first, overlapping);
  await Promise.resolve();
  let closed = false;
  const closing = scanner.close().then(() => {
    closed = true;
  });
  await Promise.resolve();
  assert.equal(closed, false);
  release();
  await Promise.all([first, overlapping, closing]);
  assert.equal(reads, 1);
  assert.equal(closes, 1);
  assert.equal(closed, true);
  assert.deepEqual(await scanner.pass(() => {}, limits), {
    entriesVisited: 0,
    complete: false,
  });
});

test("read failure closes the cursor and allows a fresh bounded retry", async () => {
  let opens = 0,
    closes = 0;
  const scanner = new BoundedDirectoryScan("synthetic", async () => {
    const attempt = ++opens;
    return {
      read: async () => {
        if (attempt === 1) throw new Error("synthetic read error");
        return null;
      },
      close: async () => {
        closes++;
      },
    };
  });
  await assert.rejects(
    scanner.pass(() => {}, limits),
    /synthetic read error/,
  );
  assert.equal(closes, 1);
  assert.equal((await scanner.pass(() => {}, limits)).complete, true);
  assert.equal(opens, 2);
  assert.equal(closes, 2);
  await scanner.close();
});

test("rejects invalid scan budgets", async () => {
  const { scanner } = fixture([]);
  for (const invalid of [0, -1, Infinity, 1.5]) {
    await assert.rejects(
      scanner.pass(() => {}, { maxEntries: invalid, maxDurationMs: 10 }),
    );
  }
  await assert.rejects(
    scanner.pass(() => {}, { maxEntries: 1, maxDurationMs: 0 }),
  );
  await scanner.close();
});
