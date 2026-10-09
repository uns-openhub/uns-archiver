import assert from "node:assert/strict";
import test from "node:test";
import { promises as fs } from "node:fs";
import path from "node:path";
import { tmpdir, hostname } from "node:os";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import {
  LegacyImportManager,
  type ImportCommand,
} from "../src/legacy-import.js";
import type { LegacyWriteResult } from "../src/legacy-import-writer.js";

async function fixture(
  t: test.TestContext,
  options: {
    write?: (event: any) => Promise<LegacyWriteResult>;
    headroom?: () => boolean;
    policy?: () => string;
    settings?: any;
  } = {},
) {
  const root = await fs.realpath(
    await fs.mkdtemp(path.join(tmpdir(), "legacy-import-test-")),
  );
  const source = path.join(root, "old");
  const live = path.join(root, "live");
  await fs.mkdir(source);
  await fs.mkdir(live);
  const managers: LegacyImportManager[] = [];
  const create = () => {
    const manager = new LegacyImportManager({
      sources: [{ id: "old-instance", directory: source }],
      liveDirectory: live,
      instanceId: "test-instance",
      policyDigest: options.policy ?? (() => "policy-1"),
      canWrite: () => true,
      hasLiveHeadroom: options.headroom ?? (() => true),
      settings: { intervalMs: 100, ...options.settings },
      write: options.write ?? (async () => ({ outcome: "written" })),
    });
    managers.push(manager);
    return manager;
  };
  t.after(async () => {
    for (const manager of managers) await manager.close();
    await fs.rm(root, { recursive: true, force: true });
  });
  const manager = create();
  const command = (action: ImportCommand["action"], revision = 0, extra = {}) =>
    manager.command({
      action,
      sourceId: "old-instance",
      expectedRevision: revision,
      requestId: `${action}-${revision}`,
      confirmSourceClosed: true,
      ...extra,
    });
  const writeFile = (
    name: string,
    value: unknown = { topic: "plant/retired/motor/speed", message: "{}" },
  ) =>
    fs.writeFile(
      path.join(source, name),
      typeof value === "string" ? value : JSON.stringify(value),
    );
  return { manager, create, root, source, live, command, writeFile };
}
const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
async function settle(manager: LegacyImportManager) {
  for (
    let pass = 0;
    pass < 20 && manager.status()[0].job?.state === "running";
    pass++
  ) {
    await manager.tick();
    if (manager.status()[0].job?.state === "running") await delay(110);
  }
}

test("start requires a configured source and explicit closed-source confirmation", async (t) => {
  const { command, manager } = await fixture(t);
  await assert.rejects(
    command("start", 0, { confirmSourceClosed: false }),
    /source-closure-confirmation-required/,
  );
  await assert.rejects(
    command("start", 0, { sourceId: "unknown" }),
    /unknown-source/,
  );
  assert.equal(manager.status()[0].job, null);
});

test("writes event and sealed old processing files in place, preserves live storage", async (t) => {
  const seen: string[] = [];
  const { manager, source, live, command, writeFile } = await fixture(t, {
    write: async (event) => {
      seen.push(event.topic);
      return { outcome: "written" };
    },
  });
  await writeFile("a.event");
  await writeFile("b.event.123.456.processing");
  await fs.writeFile(path.join(live, "fresh.event"), "live");
  await command("start");
  await settle(manager);
  assert.equal(seen.length, 2);
  assert.equal(manager.status()[0].job?.written, 2);
  assert.equal(manager.status()[0].job?.state, "completed");
  assert.deepEqual(await fs.readdir(source), [".uns-archiver-import"]);
  assert.equal(
    await fs.readFile(path.join(live, "fresh.event"), "utf8"),
    "live",
  );
});

test("checkpointed request retry is idempotent, changed reuse and stale revision fail", async (t) => {
  const { command, manager, writeFile } = await fixture(t);
  await writeFile("a.event");
  const first = await command("start");
  assert.deepEqual(await command("start"), first);
  await assert.rejects(
    command("start", 0, { confirmSourceClosed: false }),
    /request-id-conflict/,
  );
  await assert.rejects(command("pause", 0), /revision-conflict/);
  assert.equal(manager.status()[0].job?.revision, 1);
});

test("two managers cannot own, pause, or cancel the same source", async (t) => {
  const { manager, create, command, writeFile } = await fixture(t);
  await writeFile("a.event");
  await command("start");
  const peer = create();
  for (const action of ["start", "pause", "cancel"] as const) {
    await assert.rejects(
      peer.command({
        action,
        sourceId: "old-instance",
        expectedRevision: 1,
        requestId: `${action}-peer`,
        confirmSourceClosed: true,
      }),
      /source-already-claimed/,
    );
  }
  assert.equal(manager.status()[0].job?.state, "running");
});

test("pause and cached status respond while writer is pending; file stays until ACK", async (t) => {
  let finish!: () => void;
  let entered!: () => void;
  const entry = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const pending = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const { manager, command, source, writeFile } = await fixture(t, {
    write: async () => {
      entered();
      await pending;
      return { outcome: "written" };
    },
  });
  await writeFile("a.event");
  await command("start");
  const work = manager.tick();
  await entry;
  assert.equal(manager.status()[0].job?.inFlight, 1);
  for (let index = 0; index < 10_000; index++) JSON.stringify(manager.status());
  const result = await Promise.race([
    command("pause", 1),
    delay(1000).then(() => "timeout"),
  ]);
  assert.notEqual(result, "timeout");
  assert.equal(manager.status()[0].job?.state, "paused");
  assert.ok(await fs.stat(path.join(source, "a.event")));
  finish();
  await work;
  assert.equal(manager.status()[0].job?.state, "paused");
  assert.equal(manager.status()[0].job?.written, 1);
});

test("normal shutdown checkpoints paused state; restart resumes only with explicit review", async (t) => {
  const { manager, create, command, writeFile } = await fixture(t);
  await writeFile("a.event");
  await command("start");
  await manager.close();
  const restarted = create();
  await restarted.tick();
  assert.equal(restarted.status()[0].job?.state, "paused");
  assert.equal(restarted.status()[0].job?.written, 0);
  await assert.rejects(
    restarted.command({
      action: "resume",
      sourceId: "old-instance",
      expectedRevision: 1,
      requestId: "resume-without-review",
    }),
    /source-closure-confirmation-required/,
  );
  await restarted.command({
    action: "resume",
    sourceId: "old-instance",
    expectedRevision: 1,
    requestId: "resume-reviewed",
    confirmSourceClosed: true,
  });
  await restarted.tick();
  assert.equal(restarted.status()[0].job?.written, 1);
});

test("cancel preserves pending files and releases source; no rollback of confirmed rows", async (t) => {
  const { manager, command, source, writeFile } = await fixture(t);
  await writeFile("a.event");
  await command("start");
  await command("cancel", 1);
  await manager.tick();
  assert.ok(await fs.stat(path.join(source, "a.event")));
  assert.equal(manager.status()[0].ownership, "unclaimed");
  assert.equal(manager.status()[0].job?.state, "cancelled");
});

test("malformed, tmp, updated and oversized files are preserved with reasons, not completed", async (t) => {
  const { manager, source, command, writeFile } = await fixture(t, {
    settings: { maxFileBytes: 128 },
  });
  await writeFile("a.event", "broken");
  await writeFile("b.tmp", "partial");
  await writeFile("c.updated", "partial");
  await writeFile("d.event", "x".repeat(129));
  await command("start");
  await settle(manager);
  const job = manager.status()[0].job!;
  assert.equal(job.quarantined, 4);
  assert.equal(job.state, "blocked");
  assert.equal(job.lastError, "quarantine-review-required");
  const files = await fs.readdir(
    path.join(source, ".uns-archiver-import/quarantine"),
  );
  assert.equal(files.filter((file) => file.endsWith(".preserved")).length, 4);
});

test("symlink roots, overlapping live directory and duplicate source configuration are rejected", async (t) => {
  const { root, source, live } = await fixture(t);
  const link = path.join(root, "link");
  await fs.symlink(source, link);
  for (const directory of [link, live, root]) {
    const manager = new LegacyImportManager({
      sources: [{ id: "old", directory }],
      liveDirectory: live,
      instanceId: "test",
      policyDigest: () => "p",
      canWrite: () => true,
      hasLiveHeadroom: () => true,
      write: async () => ({ outcome: "written" }),
    });
    await assert.rejects(
      manager.command({
        action: "start",
        sourceId: "old",
        requestId: "start",
        expectedRevision: 0,
        confirmSourceClosed: true,
      }),
      /source-symlink-not-allowed|source-overlaps-live-storage/,
    );
    await manager.close();
  }
  assert.throws(
    () =>
      new LegacyImportManager({
        sources: [
          { id: "old", directory: source },
          { id: "old", directory: source },
        ],
        liveDirectory: live,
        instanceId: "test",
        policyDigest: () => "p",
        canWrite: () => true,
        hasLiveHeadroom: () => true,
        write: async () => ({ outcome: "written" }),
      }),
    /invalid-source-configuration/,
  );
});

test("symlink entries are retained and require review without writing their targets", async (t) => {
  const { manager, source, live, command } = await fixture(t);
  const target = path.join(live, "protected.event");
  await fs.writeFile(target, "secret");
  await fs.symlink(target, path.join(source, "a.event"));
  await command("start");
  await manager.tick();
  assert.equal(manager.status()[0].job?.written, 0);
  assert.equal(manager.status()[0].job?.state, "blocked");
  assert.equal(await fs.readFile(target, "utf8"), "secret");
});

test("missing source remains unknown and never reports empty/completed", async (t) => {
  const { manager, source } = await fixture(t);
  await fs.rmdir(source);
  manager.inspect("old-instance");
  await manager.tick();
  assert.equal(manager.status()[0].inspection.countKind, "unknown");
  assert.equal(manager.status()[0].inspection.events, null);
  assert.equal(manager.status()[0].error, "source-io-unavailable");
});

test("inspection is bounded across all entries and repeated inspection begins a fresh sweep", async (t) => {
  const { manager, writeFile } = await fixture(t);
  for (let index = 0; index < 600; index++)
    await writeFile(`${index}.tmp`, "x");
  await writeFile("queued.event");
  manager.inspect("old-instance");
  await manager.tick();
  assert.ok(manager.status()[0].inspection.entriesVisited <= 512);
  assert.equal(manager.status()[0].inspection.countKind, "lower-bound");
  for (
    let index = 0;
    index < 10 && !manager.status()[0].inspection.scanComplete;
    index++
  )
    await manager.tick();
  assert.equal(manager.status()[0].inspection.events, 1);
  manager.inspect("old-instance");
  await manager.tick();
  assert.equal(manager.status()[0].inspection.countKind, "lower-bound");
});

test("headroom reservation, batch limits and policy drift keep unconfirmed events", async (t) => {
  let headroom = false;
  let policy = "one";
  const { manager, command, source, writeFile } = await fixture(t, {
    headroom: () => headroom,
    policy: () => policy,
    settings: { batchSize: 2, concurrency: 1 },
  });
  for (let index = 0; index < 5; index++) await writeFile(`${index}.event`);
  await command("start");
  await manager.tick();
  assert.equal(manager.status()[0].job?.written, 0);
  headroom = true;
  await manager.tick();
  assert.equal(manager.status()[0].job?.written, 2);
  policy = "two";
  await delay(110);
  await manager.tick();
  assert.equal(manager.status()[0].job?.state, "blocked");
  assert.equal(manager.status()[0].job?.lastError, "storage-policy-changed");
  assert.equal(
    (await fs.readdir(source)).filter((name) => name.endsWith(".event")).length,
    3,
  );
});

test("writer failure never deletes or checkpoints a successful ACK", async (t) => {
  const { manager, command, source, writeFile } = await fixture(t, {
    write: async () => ({ outcome: "deferred", reason: "writer-unavailable" }),
  });
  await writeFile("a.event");
  await command("start");
  await manager.tick();
  assert.equal(manager.status()[0].job?.written, 0);
  assert.equal(manager.status()[0].job?.deferred, 1);
  assert.ok(await fs.stat(path.join(source, "a.event")));
  assert.notEqual(manager.status()[0].job?.state, "completed");
});

test("foreign/dead owner recovery is explicit and conservative", async (t) => {
  const { manager, command, source, writeFile } = await fixture(t);
  await writeFile("a.event");
  const lock = path.join(source, ".uns-archiver-import/lock");
  await fs.mkdir(lock, { recursive: true });
  await fs.writeFile(
    path.join(lock, "owner.json"),
    JSON.stringify({
      token: "old",
      hostname: "foreign-container",
      pid: 99999999,
      instanceId: "old",
    }),
  );
  await assert.rejects(command("start"), /source-owner-review-required/);
  await fs.writeFile(
    path.join(lock, "owner.json"),
    JSON.stringify({
      token: "old",
      hostname: hostname(),
      pid: 99999999,
      instanceId: "old",
    }),
  );
  await command("start");
  await manager.tick();
  assert.equal(manager.status()[0].job?.written, 1);
});

test("in-flight shutdown waits for writer acknowledgement before releasing claim", async (t) => {
  let finish!: () => void;
  let entered!: () => void;
  const entry = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const pending = new Promise<void>((resolve) => {
    finish = resolve;
  });
  const { manager, command, source, writeFile } = await fixture(t, {
    write: async () => {
      entered();
      await pending;
      return { outcome: "written" };
    },
  });
  await writeFile("a.event");
  await command("start");
  const work = manager.tick();
  await entry;
  let closed = false;
  const closing = manager.close().then(() => {
    closed = true;
  });
  await delay(10);
  assert.equal(closed, false);
  assert.ok(
    await fs.stat(path.join(source, ".uns-archiver-import/lock/owner.json")),
  );
  finish();
  await work;
  await closing;
  assert.equal(manager.status()[0].job?.written, 1);
  assert.equal(manager.status()[0].job?.state, "paused");
});

test("a source event changed during an acknowledged write is retained", async (t) => {
  const { manager, command, source, writeFile } = await fixture(t, {
    write: async () => {
      await fs.writeFile(path.join(source, "a.event"), "changed-source");
      return { outcome: "written" };
    },
  });
  await writeFile("a.event");
  await command("start");
  await manager.tick();
  assert.equal(manager.status()[0].job?.written, 0);
  assert.equal(manager.status()[0].job?.lastError, "source-file-changed");
  assert.equal(
    await fs.readFile(path.join(source, "a.event"), "utf8"),
    "changed-source",
  );
});

test("inaccessible state or a corrupt checkpoint cannot become an empty job", async (t) => {
  const { manager, source, command } = await fixture(t);
  const state = path.join(source, ".uns-archiver-import");
  await fs.mkdir(state);
  await fs.writeFile(path.join(state, "checkpoint.json"), "broken-json");
  await assert.rejects(command("start"), /source-io-unavailable/);
  assert.equal(manager.status()[0].job, null);
});

test("persisted quarantine survives lost counters and prevents false completion", async (t) => {
  const { manager, create, command, source, writeFile } = await fixture(t);
  await writeFile("a.event", "invalid");
  await command("start");
  await settle(manager);
  await manager.close();
  const file = path.join(source, ".uns-archiver-import/checkpoint.json");
  const checkpoint = JSON.parse(await fs.readFile(file, "utf8"));
  checkpoint.quarantined = 0;
  checkpoint.state = "paused";
  await fs.writeFile(file, JSON.stringify(checkpoint));
  const restarted = create();
  await restarted.command({
    action: "resume",
    sourceId: "old-instance",
    requestId: "resume-quarantine",
    expectedRevision: 1,
    confirmSourceClosed: true,
  });
  await settle(restarted);
  assert.equal(restarted.status()[0].job?.state, "blocked");
  assert.equal(restarted.status()[0].quarantinePresent, true);
});

test("a permanently deferred first file does not starve later entries", async (t) => {
  const seen: string[] = [];
  const { manager, command, source, writeFile } = await fixture(t, {
    settings: { batchSize: 1, concurrency: 1 },
    write: async (event) => {
      seen.push(event.topic);
      return event.topic === "bad"
        ? { outcome: "deferred", reason: "writer-unavailable" }
        : { outcome: "written" };
    },
  });
  await writeFile("a.event", { topic: "bad", message: "{}" });
  await writeFile("b.event", { topic: "good", message: "{}" });
  await command("start");
  await manager.tick();
  await delay(5100);
  await manager.tick();
  assert.ok(seen.includes("good"));
  assert.equal(manager.status()[0].job?.written, 1);
  assert.ok(await fs.stat(path.join(source, "a.event")));
});

test("forced process exit leaves a claim and file recoverable only on reviewed resume", async (t) => {
  const { manager, source, live, root, writeFile } = await fixture(t);
  await writeFile("a.event");
  const modulePath = fileURLToPath(
    new URL("../src/legacy-import.ts", import.meta.url),
  );
  const workerPath = path.join(root, "worker.mjs");
  await fs.writeFile(
    workerPath,
    `import { LegacyImportManager } from ${JSON.stringify(modulePath)};
    const manager = new LegacyImportManager({ sources: [{ id: "old-instance", directory: ${JSON.stringify(source)} }],
      liveDirectory: ${JSON.stringify(live)}, instanceId: "test-instance", policyDigest: () => "policy-1",
      canWrite: () => true, hasLiveHeadroom: () => true, write: async () => new Promise(() => {}) });
    await manager.command({ action: "start", sourceId: "old-instance", expectedRevision: 0,
      requestId: "child-start", confirmSourceClosed: true });
    process.stdout.write("claimed"); setInterval(() => {}, 1000);
    await manager.tick();`,
  );
  const loader = fileURLToPath(
    new URL("../node_modules/tsx/dist/loader.mjs", import.meta.url),
  );
  const child = spawn(process.execPath, ["--import", loader, workerPath], {
    env: {},
    stdio: ["ignore", "pipe", "pipe"],
  });
  try {
    const ready = await Promise.race([
      once(child.stdout, "data"),
      delay(3000).then(() => null),
    ]);
    assert.ok(ready, "child claimed source before forced exit");
  } finally {
    const exited = once(child, "exit");
    child.kill("SIGKILL");
    await exited;
  }
  await manager.tick();
  assert.equal(manager.status()[0].job?.state, "paused");
  assert.equal(manager.status()[0].job?.lastError, "restart-review-required");
  assert.ok(await fs.stat(path.join(source, "a.event")));
  await manager.command({
    action: "resume",
    sourceId: "old-instance",
    expectedRevision: 1,
    requestId: "resume-after-crash",
    confirmSourceClosed: true,
  });
  await settle(manager);
  assert.equal(manager.status()[0].job?.written, 1);
  assert.equal(manager.status()[0].job?.state, "completed");
});
