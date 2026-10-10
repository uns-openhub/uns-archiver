import { performance } from "node:perf_hooks";
import { errorMessage, withRetry } from "../resilience.js";

export type QuestDbBatchConfig = {
  flushIntervalMs?: number;
  maxRows?: number;
  maxPendingRows?: number;
};

export type ResolvedQuestDbBatchConfig = Required<QuestDbBatchConfig>;

export type QuestDbBatchDiagnostics = {
  queuedRows: number;
  flushingRows: number;
  successfulFlushes: number;
  failedFlushes: number;
  retriedFlushes: number;
  rejectedRows: number;
  maxBatchRowsObserved: number;
  lastBatchRows: number;
  lastFlushDurationMs: number | null;
  lastFlushAt: string | null;
  lastFailureAt: string | null;
  lastFailure: string | null;
  config: ResolvedQuestDbBatchConfig;
  queueWait: { rows: number; totalMs: number; maximumMs: number };
  totalFlushDurationMs: number;
};

export class QuestDbBatchCapacityError extends Error {
  override name = "QuestDbBatchCapacityError";

  constructor(maxPendingRows: number) {
    super(
      `QuestDB ILP batch queue is full (maxPendingRows=${maxPendingRows}).`,
    );
  }
}

type PendingRow = {
  write: () => Promise<void>;
  enqueuedAt: number;
  flushAt: number;
  resolve: () => void;
  reject: (error: unknown) => void;
};

type QuestDbIlpBatcherOptions = {
  flush: () => Promise<boolean | void>;
  reset: () => void;
  shouldRetry: (error: unknown) => boolean;
  onRetry?: (context: {
    attempt: number;
    delayMs: number;
    error: unknown;
    rows: number;
  }) => void;
  onFlush?: (context: { rows: number; durationMs: number }) => void;
  onFailure?: (context: {
    rows: number;
    durationMs: number;
    error: unknown;
  }) => void;
};

const DEFAULT_BATCH_CONFIG: ResolvedQuestDbBatchConfig = {
  flushIntervalMs: 1000,
  maxRows: 512,
  maxPendingRows: 2048,
};

const positiveInteger = (value: unknown, fallback: number): number =>
  typeof value === "number" && Number.isInteger(value) && value > 0
    ? value
    : fallback;

export const resolveQuestDbBatchConfig = (
  config?: QuestDbBatchConfig,
): ResolvedQuestDbBatchConfig => {
  const maxRows = positiveInteger(
    config?.maxRows,
    DEFAULT_BATCH_CONFIG.maxRows,
  );
  return {
    flushIntervalMs: positiveInteger(
      config?.flushIntervalMs,
      DEFAULT_BATCH_CONFIG.flushIntervalMs,
    ),
    maxRows,
    maxPendingRows: Math.max(
      maxRows,
      positiveInteger(
        config?.maxPendingRows,
        DEFAULT_BATCH_CONFIG.maxPendingRows,
      ),
    ),
  };
};

/**
 * Serializes every interaction with one QuestDB ILP Sender. The sender owns a
 * single mutable buffer, so table-local ordering alone is not sufficient.
 */
export class QuestDbIlpBatcher {
  private readonly pending: PendingRow[] = [];
  private timer: ReturnType<typeof setTimeout> | undefined;
  private timerDueAt = Infinity;
  private pendingFlushAt = Infinity;
  private queueWait = { rows: 0, totalMs: 0, maximumMs: 0 };
  private totalFlushDurationMs = 0;
  private flushInProgress: Promise<void> | undefined;
  private flushingRows = 0;
  private closed = false;
  private config: ResolvedQuestDbBatchConfig;
  private successfulFlushes = 0;
  private failedFlushes = 0;
  private retriedFlushes = 0;
  private rejectedRows = 0;
  private maxBatchRowsObserved = 0;
  private lastBatchRows = 0;
  private lastFlushDurationMs: number | null = null;
  private lastFlushAt: string | null = null;
  private lastFailureAt: string | null = null;
  private lastFailure: string | null = null;

  constructor(
    private readonly options: QuestDbIlpBatcherOptions,
    config?: QuestDbBatchConfig,
  ) {
    this.config = resolveQuestDbBatchConfig(config);
  }

  configure(config?: QuestDbBatchConfig): void {
    this.config = resolveQuestDbBatchConfig(config);
    this.clearTimer();
    for (const row of this.pending) {
      row.flushAt = Math.min(
        row.flushAt,
        row.enqueuedAt + this.config.flushIntervalMs,
      );
    }
    this.pendingFlushAt = this.pending.reduce(
      (deadline, row) => Math.min(deadline, row.flushAt),
      Infinity,
    );
    this.scheduleFlush();
  }

  enqueue(write: () => Promise<void>, maxWaitMs?: number): Promise<void> {
    if (
      maxWaitMs !== undefined &&
      (!Number.isFinite(maxWaitMs) || maxWaitMs <= 0)
    ) {
      return Promise.reject(new Error("Invalid QuestDB batch wait hint."));
    }
    if (this.closed) {
      return Promise.reject(new Error("QuestDB ILP batcher is closed."));
    }
    if (this.pending.length + this.flushingRows >= this.config.maxPendingRows) {
      return Promise.reject(
        new QuestDbBatchCapacityError(this.config.maxPendingRows),
      );
    }

    const completion = new Promise<void>((resolve, reject) => {
      const enqueuedAt = performance.now();
      const flushAt =
        enqueuedAt +
        Math.min(this.config.flushIntervalMs, maxWaitMs ?? Infinity);
      this.pending.push({ write, resolve, reject, enqueuedAt, flushAt });
      this.pendingFlushAt = Math.min(this.pendingFlushAt, flushAt);
    });
    this.scheduleFlush();
    return completion;
  }

  snapshot(): QuestDbBatchDiagnostics {
    return {
      queuedRows: this.pending.length,
      flushingRows: this.flushingRows,
      successfulFlushes: this.successfulFlushes,
      failedFlushes: this.failedFlushes,
      retriedFlushes: this.retriedFlushes,
      rejectedRows: this.rejectedRows,
      maxBatchRowsObserved: this.maxBatchRowsObserved,
      lastBatchRows: this.lastBatchRows,
      lastFlushDurationMs: this.lastFlushDurationMs,
      lastFlushAt: this.lastFlushAt,
      lastFailureAt: this.lastFailureAt,
      lastFailure: this.lastFailure,
      config: { ...this.config },
      queueWait: { ...this.queueWait },
      totalFlushDurationMs: this.totalFlushDurationMs,
    };
  }

  async close(): Promise<void> {
    this.closed = true;
    this.clearTimer();
    while (this.flushInProgress || this.pending.length > 0) {
      if (this.flushInProgress) {
        await this.flushInProgress;
      } else {
        await this.flushPending();
      }
    }
  }

  private scheduleFlush(): void {
    if (this.pending.length === 0) return;
    if (this.pending.length >= this.config.maxRows) {
      void this.flushPending();
      return;
    }
    if (this.timer && this.timerDueAt <= this.pendingFlushAt) return;
    this.clearTimer();
    this.timerDueAt = this.pendingFlushAt;
    this.timer = setTimeout(
      () => {
        this.timer = undefined;
        this.timerDueAt = Infinity;
        void this.flushPending();
      },
      Math.max(0, this.pendingFlushAt - performance.now()),
    );
  }

  private clearTimer(): void {
    if (!this.timer) return;
    clearTimeout(this.timer);
    this.timer = undefined;
    this.timerDueAt = Infinity;
  }

  private async flushPending(): Promise<void> {
    if (this.flushInProgress) return await this.flushInProgress;
    if (this.pending.length === 0) return;

    this.clearTimer();
    const batch = this.pending.splice(0, this.config.maxRows);
    this.pendingFlushAt = this.pending.reduce(
      (deadline, row) => Math.min(deadline, row.flushAt),
      Infinity,
    );
    const now = performance.now();
    for (const row of batch) {
      const wait = Math.max(0, now - row.enqueuedAt);
      this.queueWait.rows++;
      this.queueWait.totalMs += wait;
      this.queueWait.maximumMs = Math.max(this.queueWait.maximumMs, wait);
    }
    this.flushingRows = batch.length;
    let operation: Promise<void>;
    operation = this.flushBatch(batch).finally(() => {
      this.flushingRows = 0;
      if (this.flushInProgress === operation) {
        this.flushInProgress = undefined;
      }
      this.scheduleFlush();
    });
    this.flushInProgress = operation;
    await operation;
  }

  private async flushBatch(initialRows: PendingRow[]): Promise<void> {
    const startedAt = Date.now();
    let rows = initialRows;
    try {
      await withRetry(
        "QuestDB ILP batch flush",
        async () => {
          rows = await this.appendRows(rows);
          if (rows.length === 0) return;
          const flushed = await this.options.flush();
          if (flushed === false) {
            throw new Error(
              "QuestDB ILP flush completed without sending the queued rows.",
            );
          }
        },
        {
          attempts: 3,
          baseDelayMs: 120,
          maxDelayMs: 1200,
          shouldRetry: (error) => this.options.shouldRetry(error),
          onRetry: ({ attempt, delayMs, error }) => {
            this.retriedFlushes += 1;
            this.options.onRetry?.({
              attempt,
              delayMs,
              error,
              rows: rows.length,
            });
          },
        },
      );

      if (rows.length === 0) return;
      const durationMs = Date.now() - startedAt;
      this.totalFlushDurationMs += durationMs;
      this.successfulFlushes += 1;
      this.maxBatchRowsObserved = Math.max(
        this.maxBatchRowsObserved,
        rows.length,
      );
      this.lastBatchRows = rows.length;
      this.lastFlushDurationMs = durationMs;
      this.lastFlushAt = new Date().toISOString();
      this.lastFailure = null;
      for (const row of rows) row.resolve();
      this.options.onFlush?.({ rows: rows.length, durationMs });
    } catch (error) {
      const durationMs = Date.now() - startedAt;
      this.totalFlushDurationMs += durationMs;
      this.failedFlushes += 1;
      this.rejectedRows += rows.length;
      this.lastBatchRows = rows.length;
      this.lastFlushDurationMs = durationMs;
      this.lastFailureAt = new Date().toISOString();
      this.lastFailure = errorMessage(error);
      for (const row of rows) row.reject(error);
      this.options.onFailure?.({ rows: rows.length, durationMs, error });
    }
  }

  private async appendRows(rows: PendingRow[]): Promise<PendingRow[]> {
    let remaining = rows;
    while (remaining.length > 0) {
      this.options.reset();
      let failedRow: PendingRow | undefined;
      for (const row of remaining) {
        try {
          await row.write();
        } catch (error) {
          failedRow = row;
          this.rejectedRows += 1;
          row.reject(error);
          break;
        }
      }
      if (!failedRow) return remaining;
      remaining = remaining.filter((row) => row !== failedRow);
    }
    return [];
  }
}
