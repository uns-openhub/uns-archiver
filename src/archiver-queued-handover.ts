import { setTimeout as wait } from "node:timers/promises";
import type { BoundedIngestQueueSnapshot } from "./bounded-ingest-queue.js";
import type { StoredReplayDiagnostics } from "./stored-event-replay.js";
import type { QuestDbBatchDiagnostics } from "./writers/questdb-ilp-batcher.js";

type QueueCount = { pending: number | null; inFlight: number | null };
type LegacySourceStatus = {
  loaded: boolean;
  error: string | null;
  job: { state: string; inFlight: number } | null;
};

export type ArchiverQueueSnapshot = {
  live: Pick<BoundedIngestQueueSnapshot, "queuedEvents" | "processingEvents" | "spillingEvents">;
  files: StoredReplayDiagnostics;
  filesLastSpilledAt: number;
  fileStorageFault: boolean;
  targets: QueueCount & { transactions: number | null };
  legacy: readonly LegacySourceStatus[];
  historical: QueueCount;
  batches: Pick<QuestDbBatchDiagnostics, "queuedRows" | "flushingRows">;
  awaitingTopics: number;
};

export type ArchiverQueueObservation = {
  id: string;
  purpose: "application";
  unit: string;
  pending: number | null;
  inFlight: number | null;
  quality: "exact" | "lower-bound" | "unknown";
  observedAt: number;
};

/** Counts describe independent stages. They can overlap and cannot be summed
 * into a unique count of accepted MQTT messages. */
export function observeArchiverQueues(snapshot: ArchiverQueueSnapshot, now = Date.now()): ArchiverQueueObservation[] {
  const lane = (id: string, unit: string, count: QueueCount, observedAt = now): ArchiverQueueObservation => {
    const known = count.pending !== null && count.inFlight !== null;
    return {
      id, purpose: "application", unit,
      pending: known ? count.pending : null,
      inFlight: known ? count.inFlight : null,
      quality: known ? "exact" : "unknown",
      observedAt,
    };
  };
  const { live, files, targets, legacy, historical, batches } = snapshot;
  const inspection = files.queueInspection;
  const started = Date.parse(inspection.startedAt ?? "");
  const captured = Date.parse(inspection.capturedAt ?? "");
  const knownFileCount = !snapshot.fileStorageFault && !files.active && !inspection.lastError && files.failed === 0 &&
    inspection.scanComplete && inspection.countKind === "observed" &&
    Number.isFinite(captured) && captured >= started &&
    started > Math.max(snapshot.filesLastSpilledAt, files.lastMutationAt ?? 0) &&
    inspection.queuedEvents !== null && inspection.processingFiles !== null;
  const fileLane = lane("event-spool", "files", {
    pending: knownFileCount ? inspection.queuedEvents! + inspection.processingFiles! : null,
    inFlight: files.inFlight,
  }, Number.isFinite(captured) ? captured : now);
  if (!snapshot.fileStorageFault && !inspection.lastError && !inspection.scanComplete &&
      inspection.countKind === "lower-bound" && captured >= started &&
      inspection.queuedEvents !== null && inspection.queuedEvents > 0) {
    // Entries observed during this scan can already have advanced to the journal.
    fileLane.quality = "lower-bound";
    fileLane.pending = inspection.queuedEvents;
    fileLane.inFlight = files.inFlight;
  }
  const legacyKnown = legacy.every(source => source.loaded && !source.error);
  const queuedLegacy = legacy.filter(source => source.job && !["completed", "cancelled"].includes(source.job.state));
  return [
    lane("live-ingest", "events", {
      pending: live.queuedEvents, inFlight: live.processingEvents + live.spillingEvents,
    }),
    lane("unconfirmed-topics", "events", { pending: snapshot.awaitingTopics, inFlight: 0 }),
    fileLane,
    lane("target-journal", "parts", targets),
    lane("target-journal-transactions", "transactions", {
      pending: targets.transactions, inFlight: targets.transactions === null ? null : 0,
    }),
    lane("legacy-import", "jobs", {
      pending: legacyKnown ? queuedLegacy.length : null,
      inFlight: legacyKnown ? queuedLegacy.filter(source => (source.job?.inFlight ?? 0) > 0).length : null,
    }),
    lane("historical-import", "jobs", historical),
    lane("questdb-flush", "rows", { pending: batches.queuedRows, inFlight: batches.flushingRows }),
  ];
}

/** Held output is independent from existing Archiver pause controls. The SDK
 * can release it only after the predecessor's authenticated drain evidence. */
export class ArchiverHandoverEffects {
  private held: boolean;

  constructor(holdAtLaunch = false) {
    this.held = holdAtLaunch;
  }

  assertHeld() {
    if (!this.held) throw Error("Archiver target effects are already enabled.");
  }

  get canWrite() { return !this.held; }

  accept(spool: () => void, submit: () => void) {
    if (this.held) spool();
    else submit();
  }

  release() {
    this.held = false;
  }
}

/** This application-side loop must be called only after the SDK has confirmed
 * the source inlet is closed. It never transfers or clears predecessor state. */
export async function drainArchiverHandoverWork(options: {
  assertAdmissionClosed: () => void;
  scheduleExistingWork: () => Promise<void>;
  observe: () => readonly ArchiverQueueObservation[];
  signal: AbortSignal;
  intervalMs?: number;
}): Promise<void> {
  while (true) {
    options.signal.throwIfAborted();
    options.assertAdmissionClosed();
    await options.scheduleExistingWork();
    options.signal.throwIfAborted();
    options.assertAdmissionClosed();
    const lanes = options.observe();
    if (lanes.length && lanes.every(lane => lane.quality === "exact" && lane.pending === 0 && lane.inFlight === 0))
      return;
    await wait(options.intervalMs ?? 100, undefined, { signal: options.signal });
  }
}
