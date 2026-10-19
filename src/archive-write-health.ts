import type { QuestDbBatchDiagnostics } from './writers/questdb-ilp-batcher.js';
import type { BoundedIngestQueueSnapshot } from './bounded-ingest-queue.js';

/** Only bounded process-level evidence. No topics, payloads, SQL, paths or credentials. */
export function assessArchiveWriteHealth(
  batch: QuestDbBatchDiagnostics,
  ingest: BoundedIngestQueueSnapshot | undefined,
  paused: boolean,
  now = new Date(),
  disk?: {queuedEvents: number | null; processingFiles: number | null; capturedAt: string | null; scanComplete: boolean},
) {
  const pending = batch.queuedRows + batch.flushingRows;
  const thresholdMs = Math.max(5_000, batch.config.flushIntervalMs * 3);
  const overdue = pending > 0 && (batch.oldestPendingWriteMs ?? 0) >= thresholdMs;
  const writeFailed = batch.lastFailure !== null;
  const healthy = !paused && !overdue && !writeFailed;
  const age = batch.oldestPendingWriteMs === null ? 'none' : `${Math.ceil(batch.oldestPendingWriteMs / 1000)}s`;
  const message = [
    paused ? 'Archive ingestion is paused.' : writeFailed ? 'The last archive write failed; successful acknowledgement is not yet confirmed.' : overdue ? 'Archive write acknowledgement is delayed.' : pending ? 'Archive writes are pending.' : 'No pending archive writes in memory.',
    `${ingest?.pendingEvents ?? 0} memory events; ${batch.queuedRows} queued rows; ${batch.flushingRows} rows awaiting write acknowledgement.`,
    `Oldest pending write: ${age}. Last successful write acknowledgement: ${batch.lastFlushAt ?? 'none in this process'}.`,
    `Process totals: ${batch.retriedFlushes} retry attempts; ${batch.failedFlushes} failed batches; ${batch.rejectedRows} failed row attempts (including retries).`,
    disk ? `Disk queue: ${disk.queuedEvents ?? 'unknown'} observed queued files; ${disk.processingFiles ?? 'unknown'} processing files; scan ${disk.scanComplete ? 'complete' : 'in progress'}. Snapshot: ${disk.capturedAt ?? 'unavailable'}.` : 'Disk backlog observation is unavailable.',
    'Observed disk counts may be lower bounds. A write acknowledgement does not prove complete history or read-replica visibility.',
  ].join(' ');
  return {
    id: 'archiver-write-flow', label: 'Archive writes', observationMode: 'runtime-status',
    state: healthy ? 'healthy' : 'degraded', healthy, checkedAt: now.toISOString(), message,
    ...(!healthy ? {action: paused ? 'Review the ingestion pause and resume only when ready.' : 'Inspect QuestDB write latency and archiver queues; allow pending writes to drain.'} : {}),
  };
}
