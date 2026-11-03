import assert from "node:assert/strict";
import test from "node:test";
import { mkdtempSync, rmSync, readFileSync, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import {
  TargetJournal,
  TargetControlError,
  type JournalEntry,
} from "../src/target-journal.js";
import { occurrenceId, persistEventFile } from "../src/durable-event-file.js";
import { targetControlApi } from "../src/target-control-api.js";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { fileURLToPath } from "node:url";

const pause = (revision = 0, tables = ["test_paused_data"]) => ({
  action: "pause",
  expectedRevision: revision,
  requestId: randomUUID(),
  reason: "Local maintenance",
  selector: { tables },
});
const resume = (id: string, revision = 1) => ({
  action: "resume",
  expectedRevision: revision,
  requestId: randomUUID(),
  reason: "Local maintenance finished",
  pauseId: id,
});
const event = (
  table: string,
  value: number,
  id = randomUUID(),
): JournalEntry<{ value: number }> => ({
  id,
  table,
  topic: "canary/targets/value",
  payload: { value },
});
function fixture(
  options: Partial<
    ConstructorParameters<typeof TargetJournal<{ value: number }>>[0]
  > = {},
) {
  const root = mkdtempSync(path.join(tmpdir(), "archiver-target-test-"));
  const rows: JournalEntry<{ value: number }>[] = [];
  const config = {
    directory: path.join(root, "journal"),
    policyFile: path.join(root, "policy.json"),
    reserveBytes: 0,
    retryMs: 1,
    write: async (entry: JournalEntry<{ value: number }>) => {
      rows.push(entry);
    },
    ...options,
  };
  return {
    root,
    rows,
    config,
    open: () => new TargetJournal(config),
    clean: () => rmSync(root, { recursive: true, force: true }),
  };
}

test("healthy live writes may finish after source preflight; final and target checks still require drain", async () => {
  let finish!: () => void;
  const held = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const f = fixture({
    write: async () => {
      await held;
    },
  });
  const journal = f.open();
  const accepted = journal.submit([event("test_live_data", 1)]);
  try {
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(journal.snapshot().targets[0]?.inFlight, 1);
    assert.throws(() => journal.exportPolicy(), /not drained/);
    const state = journal.exportPolicy({ sourcePreflight: true });
    journal.validatePolicy(state, { sourcePreflight: true });
    assert.throws(() => journal.validatePolicy(state), /retained writes/);
    assert.throws(() => journal.importPolicy(state), /retained writes/);
    finish();
    await accepted;
    await journal.close();
    assert.deepEqual(journal.exportPolicy(), state);
    journal.validatePolicy(state);
  } finally {
    finish();
    await accepted;
    await journal.close();
    f.clean();
  }
});

test("source preflight still blocks retries and unstarted retained writes", async () => {
  const f = fixture({
    retryMs: 30_000,
    write: async () => {
      throw new Error("Database unavailable");
    },
  });
  const journal = f.open();
  try {
    await journal.submit([event("test_retry_data", 1)]);
    assert.throws(
      () => journal.exportPolicy({ sourcePreflight: true }),
      /not drained/,
    );
    assert.throws(
      () =>
        journal.validatePolicy(
          { protocol: 1, revision: 0, rules: [] },
          { sourcePreflight: true },
        ),
      /retained writes/,
    );
  } finally {
    await journal.close();
    f.clean();
  }
});
async function drain(journal: TargetJournal<any>, count = 10) {
  for (let n = 0; n < count; n++) {
    await new Promise((resolve) => setTimeout(resolve, 3));
    await journal.replay();
  }
}

test("one table remains paused while database outage spools ALL other tables; recovery is fair", async () => {
  let databaseUp = true;
  const written: JournalEntry<{ value: number }>[] = [];
  const f = fixture({
    write: async (entry) => {
      if (!databaseUp) throw new Error("Database stopped");
      written.push(entry);
    },
  });
  let journal = f.open();
  try {
    const command = pause();
    await journal.command(command, "operator");
    databaseUp = false;
    // Identical payloads are distinct occurrences; four destinations fail independently.
    const entries = Array.from({ length: 3 }, () => [
      event("test_paused_data", 7),
      event("test_b_data", 8),
      event("test_c_data", 9),
      event("test_d_table", 10),
    ]).flat();
    for (const entry of entries) await journal.submit([entry]);
    assert.equal(written.length, 0);
    assert.equal(
      journal
        .snapshot()
        .targets.reduce((n, target) => n + Number(target.queued), 0),
      12,
    );
    await journal.close();
    journal = f.open();
    assert.equal(journal.isPaused("test_paused_data"), true);
    databaseUp = true;
    await drain(journal, 20);
    assert.equal(written.length, 9);
    assert.equal(
      journal.snapshot().targets.find((t) => t.table === "test_paused_data")
        ?.queued,
      3,
    );
    assert.deepEqual(
      written
        .filter((row) => row.table === "test_b_data")
        .map((row) => row.payload.value),
      [8, 8, 8],
    );
    await journal.command(resume(command.requestId), "operator");
    await drain(journal);
    assert.equal(written.length, 12);
    assert.equal(new Set(written.map((row) => row.id)).size, 12);
    assert.equal(
      journal
        .snapshot()
        .targets.reduce((n, target) => n + Number(target.queued), 0),
      0,
    );
  } finally {
    await journal.close();
    f.clean();
  }
});

test("pause waits for actual writer completion and closes admission before its reply", async () => {
  let complete!: () => void;
  const pending = new Promise<void>((resolve) => {
    complete = resolve;
  });
  const f = fixture({ write: () => pending });
  const journal = f.open();
  try {
    const first = journal.submit([event("test_paused_data", 1)]);
    await Promise.resolve();
    const command = pause();
    let acknowledged = false;
    const control = journal.command(command, "operator").then((value) => {
      acknowledged = true;
      return value;
    });
    await journal.submit([event("test_paused_data", 2)]);
    assert.equal(acknowledged, false);
    assert.equal(journal.snapshot().boundary.inFlight, 1);
    complete();
    await first;
    const result = await control;
    assert.equal((result as any).boundary.phase, "paused");
    assert.equal(journal.snapshot().targets[0].queued, 1);
    const retry = await journal.command(command, "operator");
    assert.equal((retry as any).revision, 1);
    await assert.rejects(
      journal.command({ ...command, reason: "different" }, "operator"),
      (error: any) => error.status === 409,
    );
    await assert.rejects(
      journal.command(pause(0), "operator"),
      (error: any) => error.status === 409,
    );
    await assert.rejects(
      journal.command(command, "other-operator"),
      (error: any) => error.status === 409,
    );
  } finally {
    complete();
    await journal.close();
    f.clean();
  }
});

test("timeout never claims a drained boundary; late writes remain tracked", async () => {
  let complete!: () => void;
  const f = fixture({
    write: () =>
      new Promise<void>((resolve) => {
        complete = resolve;
      }),
  });
  const journal = f.open();
  try {
    const write = journal.submit([event("test_paused_data", 1)]);
    await Promise.resolve();
    const command = pause();
    assert.equal(
      ((await journal.command(command, "operator", 2)) as any).boundary.phase,
      "draining",
    );
    await assert.rejects(
      journal.command(resume(command.requestId), "operator"),
      (e: any) => e.status === 409,
    );
    complete();
    await write;
    assert.equal(
      ((await journal.command(command, "operator")) as any).boundary.phase,
      "paused",
    );
  } finally {
    complete();
    await journal.close();
    f.clean();
  }
});

test("branch preview resolves final shared tables and newly seen dataGroups remain paused after restart", async () => {
  const f = fixture();
  let journal = f.open();
  try {
    await journal.submit([
      { ...event("test_first_data", 1), topic: "canary/line/a" },
    ]);
    const command = { ...pause(), selector: { topicFilter: "canary/line/#" } };
    assert.deepEqual(journal.preview(command.selector).tables, [
      "test_first_data",
    ]);
    await journal.command(command, "operator");
    await journal.submit([
      { ...event("test_new_group_table", 2), topic: "canary/line/b" },
    ]);
    // A table is one write destination even when reached from another branch.
    await journal.submit([
      { ...event("test_new_group_table", 3), topic: "other/branch" },
    ]);
    await journal.close();
    journal = f.open();
    await drain(journal);
    assert.equal(f.rows.length, 1);
    assert.equal(
      journal.snapshot().targets.find((t) => t.table === "test_new_group_table")
        ?.queued,
      2,
    );
    assert.equal(journal.snapshot().policy.revision, 1);
    assert.ok(
      journal
        .snapshot()
        .policy.rules[0].tables.includes("test_new_group_table"),
    );
  } finally {
    await journal.close();
    f.clean();
  }
});

test("disk budget rejects acceptance and reports health without losing retained rows", async () => {
  const f = fixture({ maxBytes: 1048576 });
  const journal = f.open();
  try {
    await journal.command(pause(), "operator");
    await journal.submit([event("test_paused_data", 1)]);
    await assert.rejects(
      journal.submit([
        {
          ...event("test_paused_data", 2),
          payload: { value: 0, extra: "x".repeat(600000) },
        } as any,
      ]),
    );
    assert.equal(journal.snapshot().healthy, false);
    assert.equal(journal.snapshot().targets[0].queued, 1);
    assert.equal(journal.snapshot().policy.revision, 1);
  } finally {
    await journal.close();
    f.clean();
  }
});

test("one journal owner, persistent legacy receipts and equal-content source files", async () => {
  const f = fixture();
  let journal = f.open();
  try {
    assert.throws(() => f.open());
    const command = pause();
    await journal.command(command, "operator");
    const legacy = [
      event("test_paused_data", 1, "file-one:data"),
      event("test_other_data", 1, "file-one:table"),
    ].map((e) => ({ ...e, legacyKey: "file-one" }));
    assert.equal(await journal.submit(legacy), false);
    assert.equal(f.rows.length, 1);
    await journal.close();
    journal = f.open();
    assert.equal(await journal.submit(legacy), false);
    assert.equal(f.rows.length, 1);
    await journal.command(resume(command.requestId), "operator");
    await drain(journal);
    assert.equal(await journal.submit(legacy), true);
    journal.acknowledgeLegacy("file-one");
    assert.equal(f.rows.length, 2);
    await journal.submit([
      {
        ...event("test_paused_data", 1, "file-two:data"),
        legacyKey: "file-two",
      },
    ]);
    assert.equal(f.rows.length, 3);
  } finally {
    await journal.close();
    f.clean();
  }
});

test("concurrent same-table work still fills the shared batch, rather than one row per flush", async () => {
  const pending: (() => void)[] = [];
  const f = fixture({
    write: () =>
      new Promise<void>((resolve) => {
        pending.push(resolve);
      }),
  });
  const journal = f.open();
  try {
    const writes = Array.from({ length: 64 }, (_, n) =>
      journal.submit([event("test_busy_data", n)]),
    );
    await Promise.resolve();
    await Promise.resolve();
    assert.equal(pending.length, 64);
    assert.equal(journal.snapshot().targets[0].inFlight, 64);
    pending.forEach((resolve) => resolve());
    await Promise.all(writes);
  } finally {
    pending.forEach((resolve) => resolve());
    await journal.close();
    f.clean();
  }
});

test("new repeated MQTT occurrences get separate durable files; retries preserve the same file", () => {
  const root = mkdtempSync(path.join(tmpdir(), "archiver-occurrence-"));
  try {
    const first = { topic: "canary/value", message: "identical" };
    const second = { ...first };
    assert.notEqual(occurrenceId(first), occurrenceId(second));
    persistEventFile(root, first, occurrenceId(first));
    persistEventFile(root, first, occurrenceId(first));
    persistEventFile(root, second, occurrenceId(second));
    assert.equal(readdirSync(root).length, 2);
    assert.equal(
      JSON.parse(
        readFileSync(path.join(root, `${occurrenceId(first)}.event`), "utf8"),
      ).message,
      "identical",
    );
    assert.throws(() =>
      persistEventFile(
        path.join(root, "missing", "..", "..", "bad"),
        {},
        "../escape",
      ),
    );
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("operator API rejects readers and stale commands; preview makes no changes", async () => {
  const f = fixture();
  const journal = f.open();
  const api = targetControlApi(journal, () => true);
  const request = (role: string, body: unknown) => {
    let status = 200,
      result: any;
    const req = {
      headers: {
        authorization: `Bearer header.${Buffer.from(JSON.stringify({ role, sub: "test-user" })).toString("base64url")}.signature`,
      },
      body,
    };
    const res: any = {
      setHeader: () => {},
      status: (value: number) => {
        status = value;
        return res;
      },
      json: (value: any) => {
        result = value;
      },
    };
    return { event: { req, res }, value: () => ({ status, result }) };
  };
  try {
    const reader = request("user", pause());
    await api.post(reader.event);
    assert.equal(reader.value().status, 403);
    const preview = request("admin", {
      action: "preview",
      selector: { tables: ["test_paused_data"] },
    });
    await api.post(preview.event);
    assert.equal(preview.value().status, 200);
    assert.equal(journal.snapshot().policy.revision, 0);
    const operator = request("operator", pause());
    await api.post(operator.event);
    assert.equal(operator.value().status, 200);
    const stale = request("admin", pause());
    await api.post(stale.event);
    assert.equal(stale.value().status, 409);
  } finally {
    await journal.close();
    f.clean();
  }
});

test("SIGKILL preserves all accepted occurrences and pause policy without a graceful close", async () => {
  const f = fixture();
  const child = spawn(
    process.execPath,
    [
      "--import",
      "tsx",
      fileURLToPath(
        new URL("fixtures/target-journal-crash-worker.ts", import.meta.url),
      ),
      f.root,
    ],
    { stdio: ["ignore", "pipe", "pipe"] },
  );
  let stderr = "";
  child.stderr.on("data", (data) => {
    stderr += data;
  });
  let journal: TargetJournal<{ value: number }> | undefined;
  try {
    await new Promise<void>((resolve, reject) => {
      child.stdout.on("data", (data) => {
        if (String(data).includes("durably-accepted")) resolve();
      });
      child.once("exit", () => reject(new Error(stderr)));
    });
    const exited = once(child, "exit");
    child.kill("SIGKILL");
    await exited;
    journal = f.open();
    assert.equal(
      journal
        .snapshot()
        .targets.reduce((n, target) => n + Number(target.queued), 0),
      21,
    );
    assert.equal(journal.isPaused("test_paused_data"), true);
    assert.equal(journal.snapshot().boundary.phase, "write-outcome-uncertain");
    await drain(journal, 20);
    assert.equal(f.rows.length, 10);
    assert.equal(
      journal
        .snapshot()
        .targets.filter((t) => t.paused)
        .reduce((n, t) => n + Number(t.queued), 0),
      11,
    );
  } finally {
    child.kill("SIGKILL");
    await journal?.close();
    f.clean();
  }
});

test("a target pause blocks handover even with an empty queue; unrelated writes continue", async () => {
  const f = fixture();
  const journal = f.open();
  try {
    const command = pause();
    await journal.command(command, "operator");
    const before = journal.snapshot().policy;
    assert.throws(() => journal.exportPolicy(), /Resume all target pauses/);
    assert.deepEqual(journal.snapshot().policy, before);
    await journal.submit([event("test_healthy_data", 1)]);
    assert.equal(f.rows.length, 1);
    await journal.close();
    assert.throws(() => journal.exportPolicy(), /Resume all target pauses/);
  } finally {
    await journal.close();
    f.clean();
  }
});

test("a pause for an unobserved topic blocks handover and remains blocked after restart", async () => {
  const f = fixture();
  let journal = f.open();
  try {
    await journal.command(
      {
        action: "pause",
        selector: { topicFilter: "canary/future/#" },
        expectedRevision: 0,
        requestId: randomUUID(),
        reason: "Maintenance",
      },
      "operator",
    );
    assert.equal(journal.snapshot().targets.length, 0);
    assert.throws(() => journal.exportPolicy(), /target pause/);
    await journal.close();
    journal = f.open();
    assert.equal(journal.snapshot().policy.rules.length, 1);
    assert.throws(() => journal.exportPolicy(), /target pause/);
  } finally {
    await journal.close();
    f.clean();
  }
});

test("handover refuses incoming or retained pauses without replacing either policy", async () => {
  const source = fixture(),
    target = fixture();
  const a = source.open(),
    b = target.open();
  try {
    const emptyState = a.exportPolicy();
    await a.command(pause(), "operator");
    const pausedState = a.snapshot().policy; // An older peer may still offer a paused policy.
    assert.throws(() => b.validatePolicy(pausedState), /target pause/);
    assert.throws(() => b.importPolicy(pausedState), /target pause/);
    assert.deepEqual(b.snapshot().policy, emptyState);
    await b.command(pause(), "operator");
    const retained = b.snapshot().policy;
    assert.throws(() => b.importPolicy(emptyState), /target pause/);
    assert.deepEqual(b.snapshot().policy, retained);
    assert.deepEqual(a.snapshot().policy, pausedState);
  } finally {
    await a.close();
    await b.close();
    source.clean();
    target.clean();
  }
});

test("handover is allowed only after every pause is resumed and retained writes drain", async () => {
  const source = fixture(),
    target = fixture();
  const a = source.open(),
    b = target.open();
  try {
    const first = pause(),
      second = pause(1, ["test_other_data"]);
    await a.command(first, "operator");
    await a.command(second, "operator");
    await a.submit([event("test_paused_data", 12)]);
    await a.command(resume(first.requestId, 2), "operator");
    assert.throws(() => a.exportPolicy(), /target pause/);
    await a.command(resume(second.requestId, 3), "operator");
    await drain(a);
    assert.equal(source.rows.length, 1);
    const state = a.exportPolicy();
    b.importPolicy(state);
    assert.deepEqual(b.exportPolicy(), state);
    assert.equal(b.isPaused("test_paused_data"), false);
    await a.close();
    assert.deepEqual(a.exportPolicy(), state); // SDK final export follows normal drain.
    a.validatePolicy(state); // The SDK validates the final export after drain too.
    assert.throws(
      () => a.validatePolicy({ ...state, revision: state.revision + 1 }),
      /closed|different/i,
    );
    assert.throws(() => a.importPolicy(state), /closed/i);
  } finally {
    await a.close();
    await b.close();
    source.clean();
    target.clean();
  }
});

test("real SQLite write rejection propagates a durability error without acknowledging input", async () => {
  const f = fixture();
  const journal = f.open();
  try {
    await journal.command(pause(), "operator");
    (journal as any).db.exec("PRAGMA query_only=ON");
    await assert.rejects(
      journal.submit([event("test_paused_data", 12)]),
      /acceptance not confirmed/,
    );
    assert.equal(journal.snapshot().healthy, false);
    assert.equal(journal.snapshot().targets.length, 0);
    (journal as any).db.exec("PRAGMA query_only=OFF");
    await journal.submit([event("test_paused_data", 13)]);
    assert.equal(journal.snapshot().healthy, true);
    assert.equal(journal.snapshot().targets[0].queued, 1);
  } finally {
    await journal.close();
    f.clean();
  }
});

test("uncertain failed write requires explicit reviewed resume; duplicate resume is idempotent", async () => {
  let failing = true;
  const written: number[] = [];
  const f = fixture({
    write: async (entry) => {
      if (failing) throw new Error("Lost database ACK");
      written.push(entry.payload.value);
    },
  });
  const journal = f.open();
  try {
    await journal.submit([event("test_paused_data", 1)]);
    const command = pause();
    await journal.command(command, "operator");
    assert.equal(journal.snapshot().boundary.phase, "write-outcome-uncertain");
    const ordinary = resume(command.requestId);
    await assert.rejects(
      journal.command(ordinary, "operator"),
      (error: any) => error.status === 409,
    );
    failing = false;
    const reviewed = { ...ordinary, confirmUnknownWriteOutcome: true };
    assert.equal(
      ((await journal.command(reviewed, "operator")) as any).revision,
      2,
    );
    assert.equal(
      ((await journal.command(reviewed, "operator")) as any).revision,
      2,
    );
    await drain(journal);
    assert.deepEqual(written, [1]);
  } finally {
    await journal.close();
    f.clean();
  }
});

test("retained large payloads respect the in-flight byte budget while other work stays on disk", async () => {
  let release!: () => void,
    blocked = true;
  const f = fixture({
    maxInFlightBytes: 70000,
    write: async () => {
      if (blocked)
        await new Promise<void>((resolve) => {
          release = resolve;
        });
    },
  });
  const journal = f.open();
  try {
    const payload = { value: 1, padding: "x".repeat(60000) };
    const first = journal.submit([{ ...event("test_large_data", 1), payload }]);
    await Promise.resolve();
    await journal.submit([{ ...event("test_large_data", 2), payload }]);
    await journal.submit([{ ...event("test_other_data", 3), payload }]);
    assert.ok(journal.snapshot().inFlightBytes < 70000);
    assert.equal(
      journal
        .snapshot()
        .targets.reduce((n, target) => n + Number(target.queued), 0),
      3,
    );
    blocked = false;
    release();
    await first;
    await drain(journal);
    assert.equal(
      journal
        .snapshot()
        .targets.reduce((n, target) => n + Number(target.queued), 0),
      0,
    );
  } finally {
    blocked = false;
    release?.();
    await journal.close();
    f.clean();
  }
});

test("only active pauses retain the host migration policy file; SQLite keeps unpaused history", async () => {
  const f = fixture();
  let journal = f.open();
  try {
    const file = path.join(f.root, "policy.json");
    assert.equal(readdirSync(f.root).includes("policy.json"), false);
    journal.setGlobalPaused(true);
    assert.equal(JSON.parse(readFileSync(file, "utf8")).revision, 0);
    assert.throws(() => journal.exportPolicy(), /paused/);
    journal.setGlobalPaused(false);
    assert.equal(readdirSync(f.root).includes("policy.json"), false);
    const command = pause();
    await journal.command(command, "operator");
    assert.equal(JSON.parse(readFileSync(file, "utf8")).rules.length, 1);
    journal.setGlobalPaused(true);
    journal.setGlobalPaused(false);
    assert.equal(JSON.parse(readFileSync(file, "utf8")).rules.length, 1);
    await journal.command(resume(command.requestId), "operator");
    assert.equal(readdirSync(f.root).includes("policy.json"), false);
    const state = journal.exportPolicy();
    assert.equal(state.revision, 2);
    await journal.close();
    journal = f.open();
    assert.deepEqual(journal.exportPolicy(), state);
    assert.equal(readdirSync(f.root).includes("policy.json"), false);
  } finally {
    await journal.close();
    f.clean();
  }
});
