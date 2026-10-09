import type { StoredQueueSnapshot } from "./stored-event-replay.js";

export type StoredReplayControlDependencies = {
  getPaused: () => boolean;
  setPaused: (paused: boolean) => void;
  requestReplay: () => void;
  snapshot: () => StoredQueueSnapshot;
};

/** HTTP control acknowledgement never awaits inventory, recovery, or a writer flush. */
export function storedReplayControlStatus(
  action: string | undefined,
  dependencies: StoredReplayControlDependencies,
) {
  if (action === "pause") dependencies.setPaused(true);
  if (action === "resume") {
    dependencies.setPaused(false);
    dependencies.requestReplay();
  }
  const queueInspection = dependencies.snapshot();
  return {
    paused: dependencies.getPaused(),
    queuedEvents:
      queueInspection.queuedEvents === 0 && !queueInspection.scanComplete
        ? null
        : queueInspection.queuedEvents,
    queuedEventsCountKind: queueInspection.countKind,
    queueInspection,
  };
}
