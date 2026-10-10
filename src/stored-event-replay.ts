import { promises as fs } from "node:fs";
import path from "node:path";
import {
  BoundedDirectoryScan,
  DEFAULT_DIRECTORY_SCAN_LIMITS,
  type DirectoryScanLimits,
} from "./bounded-directory-scan.js";

export type StoredQueueSnapshot = {
  queuedEvents: number | null;
  processingFiles: number | null;
  unresolvedProcessingFiles: number | null;
  otherEntries: number | null;
  countKind: "unknown" | "lower-bound" | "observed";
  entriesVisited: number;
  scanComplete: boolean;
  startedAt: string | null;
  capturedAt: string | null;
  lastError: string | null;
};

export type ProcessingRecoverySnapshot = {
  scanComplete: boolean;
  entriesVisited: number;
  capturedAt: string | null;
  lastError: string | null;
};

const emptyQueueSnapshot = (): StoredQueueSnapshot => ({
  queuedEvents: null,
  processingFiles: null,
  unresolvedProcessingFiles: null,
  otherEntries: null,
  countKind: "unknown",
  entriesVisited: 0,
  scanComplete: false,
  startedAt: null,
  capturedAt: null,
  lastError: null,
});

export type StoredReplayLimits = {
  batchSize: number;
  concurrency: number;
};

export type StoredReplayDiagnostics = {
  storedQueued: number | null;
  queueInspection: StoredQueueSnapshot;
  processingRecovery: ProcessingRecoverySnapshot;
  active: boolean;
  inFlight: number;
  successful: number;
  requeued: number;
  failed: number;
  recoveredStaleProcessing: number;
  lastRunAt: string | null;
  lastSuccessAt: string | null;
  lastError: string | null;
  limits: StoredReplayLimits;
};

export type StoredEventReplayOptions = {
  eventStorageDirectory: string;
  failedStorageDirectory: string;
  eventFileExtension: string;
  processingExtension: string;
  currentProcessId?: number;
  isReady: () => boolean;
  isStopping: () => boolean;
  hasLiveHeadroom: () => boolean;
  getLimits: () => StoredReplayLimits;
  processEvent: (event: unknown) => Promise<boolean>;
  onError?: (message: string) => void;
  getScanLimits?: () => DirectoryScanLimits;
};

type LockedStoredEvent = {
  originalFileName: string;
  originalFilePath: string;
  processingFilePath: string;
};

type ProcessingFileName = {
  originalFileName: string;
  ownerPid: number;
};

const asPositiveInteger = (value: number, fallback: number): number =>
  Number.isInteger(value) && value > 0 ? value : fallback;

/**
 * Fair, bounded replay for durable event-storage files. It intentionally does
 * not add replay events to the live queue: both paths meet at the same bounded
 * QuestDB writer, while live capacity remains reserved for MQTT input.
 */
export class StoredEventReplay {
  private activeRun: Promise<void> | null = null;
  private inFlight = 0;
  private successful = 0;
  private requeued = 0;
  private failed = 0;
  private recoveredStaleProcessing = 0;
  private lastRunAt: string | null = null;
  private lastSuccessAt: string | null = null;
  private lastError: string | null = null;

  private readonly ownedProcessingFiles = new Set<string>();
  private readonly replayScan: BoundedDirectoryScan;
  private readonly inventoryScan: BoundedDirectoryScan;
  private readonly recoveryScan: BoundedDirectoryScan;
  private queueSnapshot: StoredQueueSnapshot = emptyQueueSnapshot();
  private recoverySnapshot: ProcessingRecoverySnapshot = {
    scanComplete: false,
    entriesVisited: 0,
    capturedAt: null,
    lastError: null,
  };
  private inventoryPass: Promise<void> | null = null;
  private recoveryPass: Promise<void> | null = null;
  private maintenancePass: Promise<void> | null = null;
  private maintenanceTimer: ReturnType<typeof setTimeout> | null = null;
  private maintenanceStarted = false;
  private closed = false;
  private nextInventoryAt = 0;
  private nextRecoveryAt = 0;

  constructor(private readonly options: StoredEventReplayOptions) {
    this.replayScan = new BoundedDirectoryScan(options.eventStorageDirectory);
    this.inventoryScan = new BoundedDirectoryScan(
      options.eventStorageDirectory,
    );
    this.recoveryScan = new BoundedDirectoryScan(options.eventStorageDirectory);
  }

  /** Never opens the filesystem on the caller's HTTP/status path. */
  snapshot(): StoredQueueSnapshot {
    return { ...this.queueSnapshot };
  }

  /** Schedules recovery and inventory without delaying subscriptions or API registration. */
  startBackgroundMaintenance(): void {
    if (this.maintenanceStarted || this.closed) return;
    this.maintenanceStarted = true;
    this.scheduleMaintenance(0);
  }

  private scheduleMaintenance(delayMs: number): void {
    if (this.closed) return;
    this.maintenanceTimer = setTimeout(() => {
      this.maintenanceTimer = null;
      const work = this.maintain().catch(() => {
        this.recordError("stored-replay-maintenance-failed");
      });
      this.maintenancePass = work;
      void work
        .finally(() => {
          if (this.maintenancePass === work) this.maintenancePass = null;
          this.scheduleMaintenance(100);
        })
        .catch(() => undefined);
    }, delayMs);
    this.maintenanceTimer.unref();
  }

  private async maintain(): Promise<void> {
    if (this.closed) return;
    if (Date.now() >= this.nextRecoveryAt) {
      await this.recoverStaleProcessing();
      if (
        this.recoverySnapshot.scanComplete ||
        this.recoverySnapshot.lastError
      ) {
        this.nextRecoveryAt = Date.now() + 30_000;
      }
    }
    if (!this.closed && Date.now() >= this.nextInventoryAt) {
      await this.refreshQueueSnapshot();
      if (this.queueSnapshot.scanComplete || this.queueSnapshot.lastError) {
        this.nextInventoryAt = Date.now() + 5_000;
      }
    }
  }

  async run(): Promise<void> {
    if (
      this.closed ||
      !this.options.isReady() ||
      this.options.isStopping() ||
      !this.options.hasLiveHeadroom()
    ) {
      return;
    }
    if (this.activeRun) return await this.activeRun;

    const run = this.runOnce().catch(() => {
      this.failed += 1;
      this.recordError("stored-replay-run-failed");
    });
    this.activeRun = run;
    try {
      await run;
    } finally {
      if (this.activeRun === run) this.activeRun = null;
    }
  }

  async waitForIdle(): Promise<void> {
    if (this.activeRun) await this.activeRun;
  }

  /** Stop maintenance and close all three directory handles before process exit. */
  async close(): Promise<void> {
    this.closed = true;
    if (this.maintenanceTimer) clearTimeout(this.maintenanceTimer);
    this.maintenanceTimer = null;
    await Promise.all([
      this.activeRun,
      this.maintenancePass,
      this.inventoryPass,
      this.recoveryPass,
    ]);
    await Promise.all([
      this.replayScan.close(),
      this.inventoryScan.close(),
      this.recoveryScan.close(),
    ]);
  }

  /** Compatibility helper: one bounded pass, null until a traversal finishes. */
  async countQueued(): Promise<number | null> {
    await this.refreshQueueSnapshot();
    return this.queueSnapshot.scanComplete
      ? this.queueSnapshot.queuedEvents
      : null;
  }

  /** A partial sample below the requested threshold is unknown, never zero. */
  async countQueuedUpTo(maxCount: number): Promise<number | null> {
    if (
      (!Number.isSafeInteger(maxCount) &&
        maxCount !== Number.POSITIVE_INFINITY) ||
      maxCount <= 0
    ) {
      throw new Error("maxCount must be a positive integer");
    }
    await this.refreshQueueSnapshot();
    const count = this.queueSnapshot.queuedEvents;
    if (count !== null && count >= maxCount) return maxCount;
    return this.queueSnapshot.scanComplete ? count : null;
  }

  refreshQueueSnapshot(): Promise<void> {
    if (this.closed) return Promise.resolve();
    if (this.inventoryPass) return this.inventoryPass;
    const work = this.inspectQueue();
    this.inventoryPass = work;
    void work
      .finally(() => {
        if (this.inventoryPass === work) this.inventoryPass = null;
      })
      .catch(() => undefined);
    return work;
  }

  private async inspectQueue(): Promise<void> {
    if (
      this.queueSnapshot.scanComplete ||
      this.queueSnapshot.lastError ||
      this.queueSnapshot.startedAt === null
    ) {
      this.queueSnapshot = {
        ...emptyQueueSnapshot(),
        queuedEvents: 0,
        processingFiles: 0,
        unresolvedProcessingFiles: 0,
        otherEntries: 0,
        countKind: "lower-bound",
        startedAt: new Date().toISOString(),
      };
    }
    try {
      const pass = await this.inventoryScan.pass(
        (entry) => {
          this.queueSnapshot.entriesVisited++;
          if (
            entry.isFile() &&
            entry.name.endsWith(this.options.eventFileExtension)
          ) {
            this.queueSnapshot.queuedEvents!++;
          } else if (
            entry.isFile() &&
            entry.name.endsWith(this.options.processingExtension)
          ) {
            this.queueSnapshot.processingFiles!++;
            if (!this.ownedProcessingFiles.has(entry.name)) {
              this.queueSnapshot.unresolvedProcessingFiles!++;
            }
          } else {
            this.queueSnapshot.otherEntries!++;
          }
        },
        this.scanLimits(),
        () => this.closed,
      );
      this.queueSnapshot.scanComplete = pass.complete;
      this.queueSnapshot.countKind = pass.complete ? "observed" : "lower-bound";
      this.queueSnapshot.capturedAt = new Date().toISOString();
    } catch (error) {
      // Missing/inaccessible storage is not evidence of an empty durable queue.
      this.queueSnapshot = {
        ...this.queueSnapshot,
        queuedEvents: null,
        processingFiles: null,
        unresolvedProcessingFiles: null,
        otherEntries: null,
        countKind: "unknown",
        scanComplete: false,
        capturedAt: new Date().toISOString(),
        lastError: "stored-replay-count-failed",
      };
      this.recordError("stored-replay-count-failed");
    }
  }

  recoverStaleProcessing(): Promise<void> {
    if (this.closed) return Promise.resolve();
    if (this.recoveryPass) return this.recoveryPass;
    const work = this.recoverProcessingPass();
    this.recoveryPass = work;
    void work
      .finally(() => {
        if (this.recoveryPass === work) this.recoveryPass = null;
      })
      .catch(() => undefined);
    return work;
  }

  private async recoverProcessingPass(): Promise<void> {
    if (this.recoverySnapshot.scanComplete || this.recoverySnapshot.lastError) {
      this.recoverySnapshot = {
        scanComplete: false,
        entriesVisited: 0,
        capturedAt: null,
        lastError: null,
      };
    }
    try {
      await this.ensureDirectories();
      const pass = await this.recoveryScan.pass(
        async (entry) => {
          this.recoverySnapshot.entriesVisited++;
          if (
            !entry.isFile() ||
            !entry.name.endsWith(this.options.processingExtension)
          )
            return;
          const fileName = entry.name;
          const parsed = this.parseProcessingFileName(fileName);
          if (!parsed) {
            this.failed++;
            this.recordError("stored-replay-processing-name-invalid");
            await this.moveUnrecognizedProcessingFileToFailed(fileName);
            return;
          }
          if (
            parsed.ownerPid === this.currentProcessId ||
            this.isProcessAlive(parsed.ownerPid)
          )
            return;
          const processingFilePath = path.join(
            this.options.eventStorageDirectory,
            fileName,
          );
          const originalFilePath = path.join(
            this.options.eventStorageDirectory,
            parsed.originalFileName,
          );
          try {
            if (await this.pathExists(originalFilePath)) {
              // Preserve both files: existence alone does not prove duplicate content.
              this.recordError("stored-replay-recovery-conflict");
              return;
            }
            await fs.rename(processingFilePath, originalFilePath);
            this.recoveredStaleProcessing++;
          } catch (error) {
            if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
              this.failed++;
              this.recordError("stored-replay-recovery-failed");
            }
          }
        },
        this.scanLimits(),
        () => this.closed,
      );
      this.recoverySnapshot.scanComplete = pass.complete;
      this.recoverySnapshot.capturedAt = new Date().toISOString();
    } catch {
      this.recoverySnapshot.lastError = "stored-replay-recovery-scan-failed";
      this.recoverySnapshot.capturedAt = new Date().toISOString();
      this.failed++;
      this.recordError("stored-replay-recovery-scan-failed");
    }
  }

  diagnostics(
    storedQueued: number | null = this.queueSnapshot.queuedEvents === 0 &&
    !this.queueSnapshot.scanComplete
      ? null
      : this.queueSnapshot.queuedEvents,
  ): StoredReplayDiagnostics {
    return {
      storedQueued,
      queueInspection: this.snapshot(),
      processingRecovery: { ...this.recoverySnapshot },
      active: this.activeRun !== null,
      inFlight: this.inFlight,
      successful: this.successful,
      requeued: this.requeued,
      failed: this.failed,
      recoveredStaleProcessing: this.recoveredStaleProcessing,
      lastRunAt: this.lastRunAt,
      lastSuccessAt: this.lastSuccessAt,
      lastError: this.lastError,
      limits: this.resolveLimits(),
    };
  }

  private async runOnce(): Promise<void> {
    this.lastRunAt = new Date().toISOString();
    await this.ensureDirectories();
    let eventFiles: string[];
    try {
      eventFiles = await this.selectQueuedFiles(this.resolveLimits().batchSize);
    } catch {
      this.recordError("stored-replay-scan-failed");
      return;
    }

    const locked: LockedStoredEvent[] = [];
    for (const fileName of eventFiles) {
      if (
        this.closed ||
        this.options.isStopping() ||
        !this.options.hasLiveHeadroom()
      )
        break;
      const lock = await this.lock(fileName);
      if (lock) locked.push(lock);
    }

    if (locked.length === 0) return;
    const concurrency = Math.min(
      this.resolveLimits().concurrency,
      locked.length,
    );
    let nextIndex = 0;
    const worker = async (): Promise<void> => {
      while (nextIndex < locked.length) {
        const lockedEvent = locked[nextIndex++];
        if (
          this.closed ||
          this.options.isStopping() ||
          !this.options.hasLiveHeadroom()
        ) {
          await this.requeueLocked(lockedEvent);
          this.ownedProcessingFiles.delete(
            path.basename(lockedEvent.processingFilePath),
          );
          continue;
        }
        this.inFlight += 1;
        try {
          await this.processLocked(lockedEvent);
        } finally {
          this.inFlight = Math.max(0, this.inFlight - 1);
        }
      }
    };
    await Promise.all(Array.from({ length: concurrency }, worker));
  }

  private async processLocked(lockedEvent: LockedStoredEvent): Promise<void> {
    try {
      await this.processLockedEvent(lockedEvent);
    } finally {
      this.ownedProcessingFiles.delete(
        path.basename(lockedEvent.processingFilePath),
      );
    }
  }

  private async processLockedEvent(
    lockedEvent: LockedStoredEvent,
  ): Promise<void> {
    let event: unknown;
    try {
      event = JSON.parse(
        await fs.readFile(lockedEvent.processingFilePath, "utf8"),
      );
    } catch {
      this.failed += 1;
      this.recordError("stored-replay-malformed-event");
      await this.moveToFailed(lockedEvent, "parse_error");
      return;
    }

    let processed = false;
    try {
      processed = await this.options.processEvent(event);
    } catch {
      this.recordError("stored-replay-process-failed");
    }

    if (!processed) {
      await this.persistUpdatedEvent(lockedEvent, event);
      await this.requeueLocked(lockedEvent);
      return;
    }

    try {
      await fs.unlink(lockedEvent.processingFilePath);
      this.successful += 1;
      this.lastSuccessAt = new Date().toISOString();
      this.lastError = null;
    } catch {
      this.failed += 1;
      this.recordError("stored-replay-delete-failed");
      await this.requeueLocked(lockedEvent);
    }
  }

  private async persistUpdatedEvent(
    lockedEvent: LockedStoredEvent,
    event: unknown,
  ): Promise<void> {
    const temporaryPath = `${lockedEvent.processingFilePath}.updated`;
    try {
      await fs.writeFile(temporaryPath, JSON.stringify(event), "utf8");
      await fs.rename(temporaryPath, lockedEvent.processingFilePath);
    } catch {
      this.failed += 1;
      this.recordError("stored-replay-update-failed");
      try {
        await fs.unlink(temporaryPath);
      } catch {
        // Best effort cleanup; the original locked event remains recoverable.
      }
    }
  }

  private async lock(fileName: string): Promise<LockedStoredEvent | null> {
    const originalFilePath = path.join(
      this.options.eventStorageDirectory,
      fileName,
    );
    const processingFilePath = `${originalFilePath}.${this.currentProcessId}.${Date.now()}${this.options.processingExtension}`;
    try {
      await fs.rename(originalFilePath, processingFilePath);
      this.ownedProcessingFiles.add(path.basename(processingFilePath));
      return {
        originalFileName: fileName,
        originalFilePath,
        processingFilePath,
      };
    } catch (error: any) {
      if (
        error?.code === "ENOENT" ||
        error?.code === "EPERM" ||
        error?.code === "EACCES"
      ) {
        return null;
      }
      this.failed += 1;
      this.recordError("stored-replay-lock-failed");
      return null;
    }
  }

  private async requeueLocked(lockedEvent: LockedStoredEvent): Promise<void> {
    try {
      if (await this.pathExists(lockedEvent.originalFilePath)) {
        this.recordError("stored-replay-requeue-conflict");
        return;
      } else {
        await fs.rename(
          lockedEvent.processingFilePath,
          lockedEvent.originalFilePath,
        );
      }
      this.requeued += 1;
    } catch {
      this.failed += 1;
      this.recordError("stored-replay-requeue-failed");
    }
  }

  private async moveToFailed(
    lockedEvent: LockedStoredEvent,
    reason: string,
  ): Promise<void> {
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const failedFileName = `${lockedEvent.originalFileName}.${reason}.${timestamp}`;
    try {
      await fs.rename(
        lockedEvent.processingFilePath,
        path.join(this.options.failedStorageDirectory, failedFileName),
      );
    } catch {
      this.recordError("stored-replay-failed-move-failed");
    }
  }

  private async moveUnrecognizedProcessingFileToFailed(
    fileName: string,
  ): Promise<void> {
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    try {
      await fs.rename(
        path.join(this.options.eventStorageDirectory, fileName),
        path.join(
          this.options.failedStorageDirectory,
          `${fileName}.invalid_processing_name.${timestamp}`,
        ),
      );
    } catch {
      this.recordError("stored-replay-invalid-processing-move-failed");
    }
  }

  private resolveLimits(): StoredReplayLimits {
    const limits = this.options.getLimits();
    const batchSize = asPositiveInteger(limits.batchSize, 1);
    return {
      batchSize,
      concurrency: Math.min(
        batchSize,
        asPositiveInteger(limits.concurrency, 1),
      ),
    };
  }

  private get currentProcessId(): number {
    return this.options.currentProcessId ?? process.pid;
  }

  private parseProcessingFileName(fileName: string): ProcessingFileName | null {
    const marker = `${this.options.eventFileExtension}.`;
    const markerIndex = fileName.lastIndexOf(marker);
    if (markerIndex < 0) return null;
    const originalFileName = fileName.slice(
      0,
      markerIndex + this.options.eventFileExtension.length,
    );
    const suffix = fileName.slice(markerIndex + marker.length);
    const [ownerPid] = suffix.split(".");
    const parsedPid = Number(ownerPid);
    if (!originalFileName || !Number.isInteger(parsedPid) || parsedPid <= 0)
      return null;
    return { originalFileName, ownerPid: parsedPid };
  }

  private isProcessAlive(pid: number): boolean {
    try {
      process.kill(pid, 0);
      return true;
    } catch (error: any) {
      return error?.code !== "ESRCH";
    }
  }

  private async ensureDirectories(): Promise<void> {
    await fs.mkdir(this.options.eventStorageDirectory, { recursive: true });
    await fs.mkdir(this.options.failedStorageDirectory, { recursive: true });
  }

  private scanLimits(): DirectoryScanLimits {
    return this.options.getScanLimits?.() ?? DEFAULT_DIRECTORY_SCAN_LIMITS;
  }

  private async selectQueuedFiles(limit: number): Promise<string[]> {
    const files: string[] = [];
    await this.replayScan.pass(
      (entry) => {
        if (
          entry.isFile() &&
          entry.name.endsWith(this.options.eventFileExtension)
        ) {
          files.push(entry.name);
        }
        return files.length < limit;
      },
      this.scanLimits(),
      () =>
        this.closed ||
        this.options.isStopping() ||
        !this.options.hasLiveHeadroom(),
    );
    return files;
  }

  private async pathExists(filePath: string): Promise<boolean> {
    try {
      await fs.access(filePath);
      return true;
    } catch {
      return false;
    }
  }

  private recordError(message: string): void {
    const changed = this.lastError !== message;
    this.lastError = message;
    if (changed) this.options.onError?.(message);
  }
}
