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
    acceptingCommands?: () => boolean;
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
      acceptingCommands: options.acceptingCommands,
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

test("queued import commands recheck runtime admission before changing state", async (t) => {
  let accepting = true;
  const f = await fixture(t, { acceptingCommands: () => accepting });
  const pending = f.command("start");
  accepting = false;
  await assert.rejects(pending, { code: "runtime-released" });
  assert.equal(f.manager.status()[0].job, null);
});

test("old archiver empty failed directory does not block completion or get removed", async (t) => {
  const { manager, source, command, writeFile } = await fixture(t);
  await fs.mkdir(path.join(source, "failed"));
  await writeFile("old.event");
  await command("start");
  await settle(manager);
  assert.equal(manager.status()[0].job?.state, "completed");
  assert.equal(manager.status()[0].job?.written, 1);
  assert.deepEqual(await fs.readdir(path.join(source, "failed")), []);
});

test("nonempty old failed directory is retained and requires review", async (t) => {
  const { manager, source, command, writeFile } = await fixture(t);
  await fs.mkdir(path.join(source, "failed"));
  await fs.writeFile(path.join(source, "failed", "failed.event"), "preserve");
  await writeFile("old.event");
  await command("start");
  await settle(manager);
  assert.equal(manager.status()[0].job?.state, "blocked");
  assert.equal(manager.status()[0].job?.written, 1);
  assert.equal(
    await fs.readFile(path.join(source, "failed", "failed.event"), "utf8"),
    "preserve",
  );
});

test("an empty linked failed directory is retained and never accepted as harmless", async (t) => {
  const { manager, source, live, command } = await fixture(t);
  await fs.symlink(live, path.join(source, "failed"));
  await command("start");
  await settle(manager);
  assert.equal(manager.status()[0].job?.state, "blocked");
  assert.equal(manager.status()[0].job?.written, 0);
  assert.equal(
    (await fs.lstat(path.join(source, "failed"))).isSymbolicLink(),
    true,
  );
});

test("phase diagnostics are bounded aggregates and status does not expose source paths", async (t) => {
  const f = await fixture(t);
  await f.writeFile("timed.event");
  await f.command("start");
  await settle(f.manager);
  const status = f.manager.status()[0];
  assert.equal(status.job?.state, "completed");
  assert.ok(status.performance.passes > 0);
  for (const phase of [
    "scan",
    "readParse",
    "writeAck",
    "finalize",
    "checkpoint",
  ] as const) {
    const sample = status.performance.phases[phase];
    assert.ok(sample.count > 0);
    assert.ok(sample.totalMs >= sample.maximumMs);
    assert.ok(sample.maximumMs >= 0);
  }
  assert.ok(!JSON.stringify(status).includes(f.source));
  status.performance.phases.scan.count = -1;
  assert.ok(f.manager.status()[0].performance.phases.scan.count > 0);
});

async function pendingPipeline(
  t: test.TestContext,
  extra: Parameters<typeof fixture>[1] = {},
) {
  let release!: () => void;
  const ack = new Promise<void>((resolve) => {
    release = resolve;
  });
  const writes: string[] = [];
  const f = await fixture(t, {
    ...extra,
    settings: {
      batchSize: 8,
      concurrency: 1,
      maxFileBytes: 256,
      maxBatchBytes: 1024,
      ...extra?.settings,
    },
    write: async (event) => {
      writes.push(event.topic);
      if (writes.length === 1) await ack;
      return { outcome: "written" };
    },
  });
  for (let i = 0; i < 8; i++)
    await f.writeFile(`${i}.event`, { topic: `${i}.event`, message: "{}" });
  await f.command("start");
  const work = f.manager.tick();
  try {
    for (
      let i = 0;
      i < 200 && f.manager.status()[0].performance.phases.readParse.count < 2;
      i++
    )
      await delay(5);
    assert.equal(writes.length, 1);
    assert.equal(f.manager.status()[0].performance.phases.readParse.count, 2);
  } catch (error) {
    release();
    await work;
    throw error;
  }
  // Always release before fixture teardown, including assertion failures.
  t.after(() => release());
  return { ...f, release, writes, work };
}

test("one bounded next chunk is prepared while the writer waits for ACK", async (t) => {
  const f = await pendingPipeline(t);
  try {
    const p = f.manager.status()[0].performance.pipeline;
    assert.equal(p.readAheadChunks, 1);
    assert.equal(p.maximumResidentFiles, 2);
    assert.ok(p.maximumResidentFileBytes <= 1024);
    assert.equal(f.manager.status()[0].job?.written, 0);
    assert.equal(
      (await fs.readdir(f.source)).filter((n) => n.endsWith(".event")).length,
      8,
    );
  } finally {
    f.release();
  }
  await f.work;
  await settle(f.manager);
  assert.equal(f.manager.status()[0].job?.written, 8);
  assert.equal(f.manager.status()[0].job?.state, "completed");
});

test("pause discards read-ahead and admits no new write after pause finishes", async (t) => {
  const f = await pendingPipeline(t);
  try {
    await f.command("pause", 1);
  } finally {
    f.release();
  }
  await f.work;
  assert.equal(f.writes.length, 1);
  assert.equal(f.manager.status()[0].job?.state, "paused");
  assert.equal(
    (await fs.readdir(f.source)).filter((n) => n.endsWith(".event")).length,
    7,
  );
  await f.command("resume", 2);
  await settle(f.manager);
  assert.equal(f.manager.status()[0].job?.written, 8);
});

test("shutdown drains only accepted writes and preserves prefetched files for reviewed resume", async (t) => {
  const f = await pendingPipeline(t);
  const closing = f.manager.close();
  let closed = false;
  void closing.then(() => {
    closed = true;
  });
  try {
    await delay(10);
    assert.equal(closed, false);
  } finally {
    f.release();
  }
  await f.work;
  await closing;
  assert.equal(f.writes.length, 1);
  assert.equal(f.manager.status()[0].job?.state, "paused");
  assert.equal(
    f.manager.status()[0].job?.lastError,
    "shutdown-review-required",
  );
  const peer = f.create();
  await peer.tick();
  assert.equal(peer.status()[0].job?.written, 1);
  assert.equal(peer.status()[0].job?.state, "paused");
  await peer.command({
    action: "resume",
    sourceId: "old-instance",
    expectedRevision: 1,
    requestId: "resume-pipeline",
    confirmSourceClosed: true,
  });
  await settle(peer);
  assert.equal(peer.status()[0].job?.written, 8);
});

test("loss of live headroom prevents read-ahead entering the writer", async (t) => {
  let headroom = true;
  const f = await pendingPipeline(t, { headroom: () => headroom });
  headroom = false;
  f.release();
  await f.work;
  assert.equal(f.writes.length, 1);
  assert.equal(f.manager.status()[0].job?.state, "running");
  headroom = true;
  await settle(f.manager);
  assert.equal(f.manager.status()[0].job?.written, 8);
});

test("modified read-ahead files are retained and never written from stale envelopes", async (t) => {
  const f = await pendingPipeline(t);
  try {
    for (let i = 0; i < 8; i++) {
      const name = `${i}.event`;
      if (name !== f.writes[0])
        await f.writeFile(name, {
          topic: "replacement",
          message: "changed-content",
        });
    }
  } finally {
    f.release();
  }
  await f.work;
  assert.equal(f.writes.length, 1);
  assert.equal(f.manager.status()[0].job?.lastError, "source-file-changed");
  assert.equal(
    (await fs.readdir(f.source)).filter((n) => n.endsWith(".event")).length,
    7,
  );
});

test("policy change while preparing the next chunk stops further writes", async (t) => {
  let policy = "original";
  const f = await pendingPipeline(t, { policy: () => policy });
  policy = "changed";
  f.release();
  await f.work;
  assert.equal(f.writes.length, 1);
  assert.equal(f.manager.status()[0].job?.state, "blocked");
  assert.equal(f.manager.status()[0].job?.lastError, "storage-policy-changed");
  assert.equal(
    (await fs.readdir(f.source)).filter((n) => n.endsWith(".event")).length,
    7,
  );
});

test("read-ahead residency obeys the source byte cap for large files", async (t) => {
  const f = await fixture(t, {
    settings: {
      batchSize: 8,
      concurrency: 4,
      maxFileBytes: 256,
      maxBatchBytes: 256,
    },
  });
  for (let i = 0; i < 8; i++)
    await f.writeFile(`${i}.event`, {
      topic: "large",
      message: "x".repeat(180),
    });
  await f.command("start");
  await settle(f.manager);
  const status = f.manager.status()[0];
  assert.equal(status.job?.written, 8);
  assert.equal(status.performance.pipeline.maximumResidentFiles, 1);
  assert.ok(status.performance.pipeline.maximumResidentFileBytes <= 256);
});

test("ownership loss during read-ahead prevents both deletion and further writes", async (t) => {
  const f = await pendingPipeline(t);
  const ownerFile = path.join(f.source, ".uns-archiver-import/lock/owner.json");
  const original = await fs.readFile(ownerFile, "utf8");
  try {
    const changed = JSON.parse(original);
    changed.token = "foreign-owner";
    await fs.writeFile(ownerFile, JSON.stringify(changed));
    f.release();
    await f.work;
    assert.equal(f.writes.length, 1);
    assert.equal(f.manager.status()[0].job?.written, 0);
    assert.equal(f.manager.status()[0].job?.state, "blocked");
    assert.equal(f.manager.status()[0].job?.lastError, "source-ownership-lost");
    assert.equal(
      (await fs.readdir(f.source)).filter((n) => n.endsWith(".event")).length,
      8,
    );
  } finally {
    f.release();
    await fs.writeFile(ownerFile, original);
  }
});

test("effective import settings expose the measured default and cannot be mutated through status", async (t) => {
  const f = await fixture(t);
  const settings = f.manager.status()[0].settings;
  assert.equal(settings.batchSize, 256);
  assert.equal(settings.concurrency, 64);
  assert.equal(settings.maxBatchBytes, 16 * 1024 * 1024);
  settings.batchSize = 512;
  assert.equal(f.manager.status()[0].settings.batchSize, 256);
});

test("explicit legacy batch size and pacing remain authoritative", async (t) => {
  const f = await fixture(t, { settings: { batchSize: 128, intervalMs: 500 } });
  const settings = f.manager.status()[0].settings;
  assert.equal(settings.batchSize, 128);
  assert.equal(settings.intervalMs, 500);
});

test("short pacing is explicit, bounded and does not suppress writer failure backoff", async (t) => {
  const f = await fixture(t, {
    settings: { intervalMs: 10 },
    write: async () => ({
      outcome: "deferred",
      reason: "database-unavailable",
    }),
  });
  await f.writeFile("a.event");
  await f.command("start");
  await f.manager.tick();
  const before = f.manager.status()[0];
  assert.equal(before.settings.intervalMs, 10);
  assert.equal(before.job?.deferred, 1);
  await delay(20);
  await f.manager.tick();
  assert.equal(f.manager.status()[0].job?.deferred, 1);
  assert.equal(
    await fs.readFile(path.join(f.source, "a.event"), "utf8"),
    JSON.stringify({ topic: "plant/retired/motor/speed", message: "{}" }),
  );
  await assert.rejects(
    fixture(t, { settings: { intervalMs: 9 } }),
    /invalid-import-limits/,
  );
});

test("background scheduler honors opt-in short pacing while ACK is still required", async (t) => {
  const f = await fixture(t, {
    settings: { intervalMs: 10, batchSize: 1, concurrency: 1 },
  });
  await f.writeFile("a.event");
  await f.writeFile("b.event");
  await f.command("start");
  f.manager.startBackground();
  for (
    let i = 0;
    i < 100 && f.manager.status()[0].job?.state === "running";
    i++
  )
    await delay(10);
  assert.equal(f.manager.status()[0].job?.written, 2);
  assert.equal(f.manager.status()[0].job?.state, "completed");
});
