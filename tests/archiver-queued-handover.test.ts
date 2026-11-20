import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { setTimeout as wait } from "node:timers/promises";
import {
  ArchiverHandoverEffects,
  drainArchiverHandoverWork,
  observeArchiverQueues,
  type ArchiverQueueSnapshot,
} from "../src/archiver-queued-handover.js";
import { BoundedIngestQueue } from "../src/bounded-ingest-queue.js";
import { persistEventFile } from "../src/durable-event-file.js";
import { StoredEventReplay } from "../src/stored-event-replay.js";
import { deferredQuestDbSender } from "../src/writers/deferred-questdb-sender.js";
import { QuestDBWriter } from "../src/writers/questDbWriter.js";

const start = 1_793_650_000_000;
const at = (ms: number) => new Date(start + ms).toISOString();

function snapshot(): ArchiverQueueSnapshot {
  return {
    live: { queuedEvents: 4, processingEvents: 2, spillingEvents: 1 },
    files: {
      storedQueued: 6,
      queueInspection: {
        scanComplete: true, countKind: "observed", startedAt: at(3), capturedAt: at(4),
        queuedEvents: 6, processingFiles: 2, unresolvedProcessingFiles: 1, otherEntries: 0,
        entriesVisited: 8, lastError: null,
      },
      processingRecovery: { scanComplete: true, entriesVisited: 8, capturedAt: at(4), lastError: null },
      active: false, inFlight: 0, successful: 7, requeued: 0, failed: 0,
      recoveredStaleProcessing: 0, lastRunAt: at(2), lastMutationAt: start + 2, lastSuccessAt: at(2), lastError: null,
      limits: { batchSize: 8, concurrency: 2 },
    },
    filesLastSpilledAt: start + 1,
    fileStorageFault: false,
    targets: { pending: 9, inFlight: 2, transactions: 1 },
    legacy: [
      { loaded: true, job: { state: "running", inFlight: 9 }, error: null },
      { loaded: true, job: { state: "paused", inFlight: 0 }, error: null },
      { loaded: true, job: { state: "completed", inFlight: 0 }, error: null },
    ],
    historical: { pending: 1, inFlight: 1 },
    batches: { queuedRows: 3, flushingRows: 5 },
    awaitingTopics: 1,
  };
}

test("observes app work by queue with accurate units and timestamps", () => {
  const status = observeArchiverQueues(snapshot(), start + 5);
  assert.deepEqual(status.map(({ id, unit, pending, inFlight }) => [id, unit, pending, inFlight]), [
    ["live-ingest", "events", 4, 3],
    ["unconfirmed-topics", "events", 1, 0],
    ["event-spool", "files", 8, 0],
    ["target-journal", "parts", 9, 2],
    ["target-journal-transactions", "transactions", 1, 0],
    ["legacy-import", "jobs", 2, 1],
    ["historical-import", "jobs", 1, 1],
    ["questdb-flush", "rows", 3, 5],
  ]);
  assert.equal(status.find(({ id }) => id === "event-spool")?.observedAt, start + 4);
  assert.ok(status.every(({ purpose, quality }) => purpose === "application" && quality === "exact"));
});

test("a partial spool scan shows a measured lower bound and never reports zero as empty", () => {
  const data = snapshot();
  const scan = data.files.queueInspection;
  scan.scanComplete = false;
  scan.countKind = "lower-bound";
  scan.queuedEvents = 8192;
  const sample = () => observeArchiverQueues(data, start + 5).find(lane => lane.id === "event-spool");
  assert.deepEqual([sample()?.quality, sample()?.pending, sample()?.observedAt], ["lower-bound", 8192, start + 4]);
  scan.queuedEvents = 0;
  assert.equal(sample()?.quality, "unknown");
  assert.equal(sample()?.pending, null);
});

test("a missed/ambiguous scan, failed target journal and unloaded legacy job never become an empty lane", () => {
  const status = observeArchiverQueues({
    ...snapshot(),
    filesLastSpilledAt: start + 3,
    targets: { pending: null, inFlight: null, transactions: null },
    legacy: [{ loaded: false, job: null, error: "source-io-unavailable" }],
  }, start + 5);
  for (const id of ["event-spool", "target-journal", "target-journal-transactions", "legacy-import"]) {
    const queue = status.find(lane => lane.id === id);
    assert.deepEqual([queue?.quality, queue?.pending, queue?.inFlight], ["unknown", null, null]);
  }
});

test("the drainer requires closed admission and treats unknown data as outstanding through timeout", async () => {
  let closed = false, passes = 0;
  const options: Parameters<typeof drainArchiverHandoverWork>[0] = {
    assertAdmissionClosed: () => { if (!closed) throw Error("Source admission open"); },
    scheduleExistingWork: async () => { passes++; },
    observe: () => [{
      id: "event-spool", purpose: "application", unit: "files",
      pending: null, inFlight: null, quality: "unknown", observedAt: Date.now(),
    }],
    signal: AbortSignal.timeout(35),
    intervalMs: 1,
  };
  await assert.rejects(drainArchiverHandoverWork(options), /Source admission open/);
  assert.equal(passes, 0);
  closed = true;
  await assert.rejects(drainArchiverHandoverWork(options), error =>
    error instanceof Error && (error.name === "AbortError" || error.name === "TimeoutError"));
  assert.ok(passes > 0);
});

test("the Archiver entrypoint rejects a forced queued launch for ordinary MQTT", () => {
  const working = mkdtempSync(path.join(tmpdir(), "archiver-queued-launch-"));
  try {
    writeFileSync(path.join(working, "package.json"), JSON.stringify({ name: "archiver-queued-launch-test", version: "0.0.0" }));
    const processResult = spawnSync(process.execPath, [
      "--import", import.meta.resolve("tsx"), "--input-type=module", "-e",
      "await import(process.argv[1]).catch(error => { console.error(error.message); process.exitCode = 1; });",
      new URL("../src/index.ts", import.meta.url).href,
    ], {
      cwd: working, env: { ...process.env, UNS_QUEUED_HANDOVER_ROLE: "target" }, encoding: "utf8",
      timeout: 5000,
    });
    assert.equal(processResult.status, 1, processResult.stderr);
    assert.match(processResult.stderr, /Archiver MQTT does not support queued handover target activation/);
    assert.equal(readdirSync(working).includes("event_storage"), false);
  } finally {
    rmSync(working, { recursive: true, force: true });
  }
});

const endpoint = process.env.ARCHIVER_TEST_QUESTDB;
test("held Archiver capture survives restart; predecessor and successor write real QuestDB rows in order",
  { skip: !endpoint, timeout: 30_000 }, async () => {
    const url = new URL(endpoint!);
    assert.equal(url.protocol, "http:");
    assert.equal(url.hostname, "127.0.0.1");
    assert.equal(url.pathname, "/");
    const root = mkdtempSync(path.join(tmpdir(), "archiver-queued-handover-"));
    const unique = `test_handover_${randomUUID().replaceAll("-", "")}`;
    const config = `http::addr=127.0.0.1:${url.port};retry_timeout=500;request_timeout=1000;`;
    const runtimes: ReturnType<typeof open>[] = [];
    const packet = (id: number) => ({
      version: "2.0.0", message: { data: { value: id, uom: "kWh", time: at(id * 1000) } },
    });
    const event = (id: number) => ({ id: `evt-${id}`, value: id, topic: "canary/queue/value", message: packet(id) });
    type Event = ReturnType<typeof event>;

    function open(owner: "source" | "target", held: boolean) {
      const directory = path.join(root, owner, "events");
      const prefix = `${unique}_${owner}`;
      const effects = new ArchiverHandoverEffects(held);
      mkdirSync(directory, { recursive: true, mode: 0o700 });
      let backendEnabled = true, lastStoredAt = 0, closed = false;
      const writer = new QuestDBWriter(deferredQuestDbSender(config), config, { maxRows: 4, flushIntervalMs: 5 });
      const journal = writer.configureTargetJournal({
        directory: path.join(directory, "targets"), policyFile: path.join(root, owner, "policy"),
        canReplay: () => effects.canWrite && backendEnabled, reserveBytes: 0,
      });
      const submit = async (input: Event) => {
        await writer.writeUnsPacket(input.message, prefix, input.topic, undefined, undefined, undefined, { occurrenceId: input.id });
      };
      const spool = (input: Event) => { persistEventFile(directory, input, input.id); lastStoredAt = Date.now(); };
      const queue = new BoundedIngestQueue<Event>({
        concurrency: 2, maxPendingEvents: 4, maxPendingBytes: 200_000,
        process: item => submit(item.value), spill: async item => spool(item.value),
      });
      const replay = new StoredEventReplay({
        eventStorageDirectory: directory, failedStorageDirectory: path.join(directory, "failed"),
        eventFileExtension: ".event", processingExtension: ".processing",
        isReady: () => true, isStopping: () => !effects.canWrite, hasLiveHeadroom: () => true,
        getLimits: () => ({ batchSize: 4, concurrency: 2 }),
        processEvent: async input => { await submit(input as Event); return true; },
      });
      const result = {
        directory, prefix, effects, submit, spool, replay, journal,
        setBackendEnabled: (enabled: boolean) => { backendEnabled = enabled; },
        admit: (input: Event) => effects.accept(() => spool(input),
          () => { queue.enqueue({ id: input.id, value: input, bytes: Buffer.byteLength(JSON.stringify(input)) }); }),
        observe: () => observeArchiverQueues({
          live: queue.snapshot(), files: replay.diagnostics(), filesLastSpilledAt: lastStoredAt,
          fileStorageFault: false, targets: journal.queuedHandoverWork(), legacy: [],
          historical: { pending: 0, inFlight: 0 }, batches: writer.getBatchDiagnostics(), awaitingTopics: 0,
        }),
        work: async () => {
          await queue.waitForIdle();
          await journal.replay();
          await replay.run();
          await replay.refreshQueueSnapshot();
        },
        close: async () => {
          if (closed) return;
          closed = true;
          await queue.waitForIdle();
          await replay.close();
          await journal.close();
          await writer.close();
        },
      };
      runtimes.push(result);
      return result;
    }

    const values = async (prefix: string): Promise<number[]> => {
      const sql = `select numberValue from ${prefix}_data order by timestamp`;
      const result = await fetch(new URL(`/exec?query=${encodeURIComponent(sql)}`, url), { signal: AbortSignal.timeout(2000) });
      const body = await result.json() as { dataset?: [number][]; error?: string };
      if (result.ok) return (body.dataset ?? []).map(([value]) => value);
      assert.match(body.error ?? "", /does not exist|not found/i);
      return [];
    };
    const awaitValues = async (prefix: string, expected: number[]) => {
      for (let attempt = 0; attempt < 40; attempt++) {
        const actual = await values(prefix);
        if (actual.length === expected.length) {
          assert.deepEqual(actual, expected);
          return;
        }
        await wait(50);
      }
      assert.deepEqual(await values(prefix), expected);
    };

    try {
      const source = open("source", false);
      let target = open("target", true);
      source.spool(event(1));
      source.setBackendEnabled(false);
      await source.submit(event(2));
      assert.equal(source.journal.queuedHandoverWork().pending, 1);

      target.effects.assertHeld();
      target.admit(event(4));
      // A direct writer attempt still reaches only the fenced target journal.
      await target.submit(event(5));
      await target.work();
      assert.deepEqual(await values(target.prefix), []);
      assert.equal(target.journal.queuedHandoverWork().pending, 1);
      assert.ok(readdirSync(target.directory).includes("evt-4.event"));

      await target.close();
      target = open("target", true);
      target.effects.assertHeld();
      target.admit(event(6));
      await target.work();
      assert.deepEqual(await values(target.prefix), []);
      assert.deepEqual(readdirSync(target.directory).filter(file => file.endsWith(".event")).sort(),
        ["evt-4.event", "evt-6.event"]);

      source.setBackendEnabled(true);
      source.admit(event(3));
      const admissionClosed = true;  // The fixture has issued every source occurrence.
      await drainArchiverHandoverWork({
        assertAdmissionClosed: () => assert.equal(admissionClosed, true),
        scheduleExistingWork: source.work,
        observe: source.observe,
        signal: AbortSignal.timeout(5000),
        intervalMs: 5,
      });
      await awaitValues(source.prefix, [1, 2, 3]);
      assert.deepEqual(await values(target.prefix), []);

      // Represents the SDK's verified release callback, not a broker-cutover proof.
      target.effects.release();
      await target.work();
      await awaitValues(target.prefix, [4, 5, 6]);
      assert.equal(target.journal.queuedHandoverWork().pending, 0);
      assert.equal(readdirSync(target.directory).filter(file => file.endsWith(".event")).length, 0);
    } finally {
      await Promise.all(runtimes.map(runtime => runtime.close()));
      rmSync(root, { recursive: true, force: true });
    }
  });
