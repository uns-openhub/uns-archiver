export type BoundedIngestQueueItem<T> = {
  id: string;
  value: T;
  bytes: number;
};

export type BoundedIngestQueueLimits = {
  maxPendingEvents: number;
  maxPendingBytes: number;
  concurrency: number;
};

export type BoundedIngestQueueSnapshot = {
  pendingEvents: number;
  pendingBytes: number;
  queuedEvents: number;
  processingEvents: number;
  spillingEvents: number;
  overflowSpilled: number;
  duplicateSuppressed: number;
  processFailures: number;
  limits: BoundedIngestQueueLimits;
};

export type BoundedIngestQueueEnqueueResult =
  "queued" | "spilled" | "duplicate";

export type BoundedIngestQueueOptions<T> = BoundedIngestQueueLimits & {
  process: (item: BoundedIngestQueueItem<T>) => Promise<void>;
  spill: (
    item: BoundedIngestQueueItem<T>,
    reason: "overflow" | "process_error",
  ) => Promise<void>;
  onProcessError?: (item: BoundedIngestQueueItem<T>, error: unknown) => void;
  onSpillError?: (
    item: BoundedIngestQueueItem<T>,
    reason: "overflow" | "process_error",
    error: unknown,
  ) => void;
};

const normalizePositiveInteger = (value: number, fallback: number): number =>
  Number.isInteger(value) && value > 0 ? value : fallback;

/**
 * Bounds live ingestion work so a slow downstream writer cannot retain an
 * unbounded number of MQTT payloads in memory. Overflow is handed to the
 * caller's durable spool immediately and is not retried in memory.
 */
export class BoundedIngestQueue<T> {
  private readonly queued: BoundedIngestQueueItem<T>[] = [];
  private readonly pendingIds = new Set<string>();
  private readonly idleWaiters = new Set<() => void>();
  private active = 0;
  private pendingBytes = 0;
  private spilling = 0;
  private overflowSpilled = 0;
  private duplicateSuppressed = 0;
  private processFailures = 0;
  private limits: BoundedIngestQueueLimits;

  constructor(private readonly options: BoundedIngestQueueOptions<T>) {
    this.limits = this.normalizeLimits(options);
  }

  configure(limits: Partial<BoundedIngestQueueLimits>): void {
    this.limits = this.normalizeLimits({ ...this.limits, ...limits });
    this.drain();
  }

  enqueue(item: BoundedIngestQueueItem<T>): BoundedIngestQueueEnqueueResult {
    if (this.pendingIds.has(item.id)) {
      this.duplicateSuppressed += 1;
      return "duplicate";
    }

    const normalizedItem = {
      ...item,
      bytes: Math.max(0, Math.trunc(item.bytes)),
    };
    this.pendingIds.add(normalizedItem.id);

    if (!this.canAccept(normalizedItem)) {
      this.overflowSpilled += 1;
      this.beginSpill(normalizedItem, "overflow");
      return "spilled";
    }

    this.queued.push(normalizedItem);
    this.pendingBytes += normalizedItem.bytes;
    this.drain();
    return "queued";
  }

  snapshot(): BoundedIngestQueueSnapshot {
    return {
      pendingEvents: this.queued.length + this.active,
      pendingBytes: this.pendingBytes,
      queuedEvents: this.queued.length,
      processingEvents: this.active,
      spillingEvents: this.spilling,
      overflowSpilled: this.overflowSpilled,
      duplicateSuppressed: this.duplicateSuppressed,
      processFailures: this.processFailures,
      limits: { ...this.limits },
    };
  }

  async waitForIdle(): Promise<void> {
    if (this.isIdle()) return;
    await new Promise<void>((resolve) => this.idleWaiters.add(resolve));
  }

  private normalizeLimits(
    limits: BoundedIngestQueueLimits,
  ): BoundedIngestQueueLimits {
    return {
      maxPendingEvents: normalizePositiveInteger(limits.maxPendingEvents, 1),
      maxPendingBytes: normalizePositiveInteger(limits.maxPendingBytes, 1),
      concurrency: normalizePositiveInteger(limits.concurrency, 1),
    };
  }

  private canAccept(item: BoundedIngestQueueItem<T>): boolean {
    const pendingEvents = this.queued.length + this.active;
    return (
      pendingEvents < this.limits.maxPendingEvents &&
      this.pendingBytes + item.bytes <= this.limits.maxPendingBytes
    );
  }

  private drain(): void {
    while (this.active < this.limits.concurrency && this.queued.length > 0) {
      const item = this.queued.shift()!;
      this.active += 1;
      void this.process(item);
    }
  }

  private async process(item: BoundedIngestQueueItem<T>): Promise<void> {
    try {
      await this.options.process(item);
      this.pendingIds.delete(item.id);
    } catch (error) {
      this.processFailures += 1;
      this.options.onProcessError?.(item, error);
      await this.spill(item, "process_error");
    } finally {
      this.active = Math.max(0, this.active - 1);
      this.pendingBytes = Math.max(0, this.pendingBytes - item.bytes);
      this.drain();
      this.notifyIdleWaiters();
    }
  }

  private beginSpill(
    item: BoundedIngestQueueItem<T>,
    reason: "overflow" | "process_error",
  ): void {
    void this.spill(item, reason).finally(() => this.notifyIdleWaiters());
  }

  private async spill(
    item: BoundedIngestQueueItem<T>,
    reason: "overflow" | "process_error",
  ): Promise<void> {
    this.spilling += 1;
    try {
      await this.options.spill(item, reason);
    } catch (error) {
      this.options.onSpillError?.(item, reason, error);
    } finally {
      this.spilling = Math.max(0, this.spilling - 1);
      this.pendingIds.delete(item.id);
    }
  }

  private isIdle(): boolean {
    return this.queued.length === 0 && this.active === 0 && this.spilling === 0;
  }

  private notifyIdleWaiters(): void {
    if (!this.isIdle()) return;
    for (const resolve of this.idleWaiters) resolve();
    this.idleWaiters.clear();
  }
}
