type Pending = {
  work: () => void;
  resolve: () => void;
  reject: (error: unknown) => void;
};

/** Share one FULL SQLite commit without acknowledging work before it commits. */
export class DurableTransactions {
  private pending: Pending[] = [];
  private scheduled = false;

  constructor(private readonly transaction: (work: () => void) => void) {}

  get size(): number { return this.pending.length; }

  run(work: () => void): Promise<void> {
    const result = new Promise<void>((resolve, reject) => {
      this.pending.push({ work, resolve, reject });
    });
    if (!this.scheduled) {
      this.scheduled = true;
      queueMicrotask(() => this.flush());
    }
    return result;
  }

  private flush(): void {
    this.scheduled = false;
    const batch = this.pending.splice(0, 128);
    try {
      this.transaction(() => { for (const operation of batch) operation.work(); });
      for (const operation of batch) operation.resolve();
    } catch (error) {
      for (const operation of batch) operation.reject(error);
    }
    if (this.pending.length && !this.scheduled) {
      this.scheduled = true;
      queueMicrotask(() => this.flush());
    }
  }

  async drain(): Promise<void> {
    // A marker is committed after earlier operations; their continuations can
    // enqueue completion receipts, so close also waits for the tracked writers.
    if (this.pending.length) await this.run(() => {});
  }
}
