import { promises as fs } from "node:fs";
import path from "node:path";

export type StoredReplayLimits = {
  batchSize: number;
  concurrency: number;
};

export type StoredReplayDiagnostics = {
  storedQueued: number;
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

  constructor(private readonly options: StoredEventReplayOptions) {}

  async run(): Promise<void> {
    if (
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

  async countQueued(): Promise<number> {
    try {
      let count = 0;
      const directory = await fs.opendir(this.options.eventStorageDirectory);
      for await (const entry of directory) {
        if (path.extname(entry.name) === this.options.eventFileExtension) {
          count += 1;
        }
      }
      return count;
    } catch (error: any) {
      if (error?.code === "ENOENT") return 0;
      this.recordError("stored-replay-count-failed");
      return 0;
    }
  }

  async recoverStaleProcessing(): Promise<void> {
    try {
      await this.ensureDirectories();
    } catch {
      this.failed += 1;
      this.recordError("stored-replay-recovery-setup-failed");
      return;
    }
    let directory;
    try {
      directory = await fs.opendir(this.options.eventStorageDirectory);
    } catch {
      this.recordError("stored-replay-recovery-scan-failed");
      return;
    }

    try {
      for await (const entry of directory) {
        const fileName = entry.name;
        if (!fileName.endsWith(this.options.processingExtension)) continue;
        const parsed = this.parseProcessingFileName(fileName);
        if (!parsed) {
          this.failed += 1;
          this.recordError("stored-replay-processing-name-invalid");
          await this.moveUnrecognizedProcessingFileToFailed(fileName);
          continue;
        }
        if (
          parsed.ownerPid === this.currentProcessId ||
          this.isProcessAlive(parsed.ownerPid)
        ) {
          continue;
        }

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
            await fs.unlink(processingFilePath);
          } else {
            await fs.rename(processingFilePath, originalFilePath);
          }
          this.recoveredStaleProcessing += 1;
        } catch {
          this.failed += 1;
          this.recordError("stored-replay-recovery-failed");
        }
      }
    } catch {
      this.recordError("stored-replay-recovery-scan-failed");
    }
  }

  diagnostics(storedQueued: number): StoredReplayDiagnostics {
    return {
      storedQueued,
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
      eventFiles = await this.selectQueuedFiles(
        this.resolveLimits().batchSize,
      );
    } catch {
      this.recordError("stored-replay-scan-failed");
      return;
    }

    const locked: LockedStoredEvent[] = [];
    for (const fileName of eventFiles) {
      if (this.options.isStopping() || !this.options.hasLiveHeadroom()) break;
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
        if (this.options.isStopping() || !this.options.hasLiveHeadroom()) {
          await this.requeueLocked(lockedEvent);
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
        await fs.unlink(lockedEvent.processingFilePath);
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

  private async selectQueuedFiles(limit: number): Promise<string[]> {
    const files: string[] = [];
    const directory = await fs.opendir(this.options.eventStorageDirectory);
    for await (const entry of directory) {
      if (path.extname(entry.name) !== this.options.eventFileExtension) {
        continue;
      }
      files.push(entry.name);
      if (files.length >= limit) break;
    }
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
    this.lastError = message;
    this.options.onError?.(message);
  }
}
