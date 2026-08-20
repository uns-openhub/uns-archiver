import type { StoredReplayLimits } from "./stored-event-replay.js";

export type LiveIngestReplaySnapshot = {
  pendingEvents: number;
  pendingBytes: number;
  spillingEvents: number;
};

export const resolveStoredReplayLimits = (
  ingestQueueMaxEvents: number,
  storedReplayBatchSize: number,
): StoredReplayLimits => {
  const maximumBatchSize = Math.max(1, Math.floor(ingestQueueMaxEvents / 2));
  const batchSize = Math.min(storedReplayBatchSize, maximumBatchSize);
  return {
    batchSize,
    concurrency: Math.min(
      batchSize,
      Math.max(1, Math.floor(ingestQueueMaxEvents / 8)),
    ),
  };
};

export const hasStoredReplayLiveHeadroom = (
  snapshot: LiveIngestReplaySnapshot | undefined,
  ingestQueueMaxEvents: number,
  ingestQueueMaxBytes: number,
): boolean => {
  if (!snapshot) return true;
  const eventReserve = Math.min(
    Math.max(0, ingestQueueMaxEvents - 1),
    Math.max(1, Math.ceil(ingestQueueMaxEvents * 0.25)),
  );
  const byteReserve = Math.min(
    Math.max(0, ingestQueueMaxBytes - 1),
    Math.max(1, Math.ceil(ingestQueueMaxBytes * 0.25)),
  );
  return (
    snapshot.spillingEvents === 0 &&
    snapshot.pendingEvents < ingestQueueMaxEvents - eventReserve &&
    snapshot.pendingBytes < ingestQueueMaxBytes - byteReserve
  );
};
