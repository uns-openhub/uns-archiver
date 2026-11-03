import type { ApplicationHandoverState } from "@uns-kit/core";
import type { StoredReplayDiagnostics } from "./stored-event-replay.js";

export function assertStoredQueueDrainedForHandover(
  status: StoredReplayDiagnostics,
  lastDurableFileAt = 0,
): void {
  const queue = status.queueInspection;
  // A scan that started before a spill can finish later without seeing it.
  // Equal millisecond timestamps cannot prove ordering either.
  const startedAt = Date.parse(queue.startedAt ?? "");
  const observedAt = Date.parse(queue.capturedAt ?? "");
  if (
    status.active ||
    status.inFlight !== 0 ||
    !queue.scanComplete ||
    queue.countKind !== "observed" ||
    queue.lastError ||
    queue.queuedEvents !== 0 ||
    queue.processingFiles !== 0 ||
    queue.unresolvedProcessingFiles !== 0 ||
    !Number.isFinite(startedAt) ||
    !Number.isFinite(observedAt) ||
    observedAt < startedAt ||
    startedAt <= lastDurableFileAt
  )
    throw new Error(
      "Archiver file queue is not confirmed empty. Keep the source active and let queue inspection/replay finish before migration.",
    );
}

type PolicyOwner = {
  exportPolicy(options?: { sourcePreflight?: boolean }): unknown;
  validatePolicy(value: unknown, options?: { sourcePreflight?: boolean }): void;
  importPolicy(value: unknown): void;
};

/** The SDK transports state; Archiver decides whether its own work may move. */
export function createArchiverHandoverState(options: {
  contract: string;
  isPaused: () => boolean;
  isSourceActive?: () => boolean;
  journal: PolicyOwner;
  assertSourceReady?: () => void;
}): ApplicationHandoverState {
  const requireRunning = () => {
    if (options.isPaused())
      throw new Error(
        "Archiver migration is blocked while archiving is paused. Resume archiving before migration.",
      );
  };
  return {
    contract: options.contract,
    exportState() {
      requireRunning();
      options.assertSourceReady?.();
      return options.journal.exportPolicy({
        sourcePreflight: options.isSourceActive?.() === true,
      });
    },
    validateState(value) {
      requireRunning();
      options.journal.validatePolicy(value, {
        sourcePreflight: options.isSourceActive?.() === true,
      });
    },
    importState(value) {
      requireRunning();
      options.journal.importPolicy(value);
    },
  };
}
