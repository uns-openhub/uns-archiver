import { constants, promises as fs } from "node:fs";
import path from "node:path";
import { hostname } from "node:os";
import { createHash, randomUUID } from "node:crypto";
import { BoundedDirectoryScan } from "./bounded-directory-scan.js";
import type { LegacyEvent, LegacyWriteResult } from "./legacy-import-writer.js";

const STATE_DIRECTORY = ".uns-archiver-import";
const ID = /^[a-zA-Z0-9][a-zA-Z0-9-]{0,63}$/;
const EVENT = /\.event(?:\.\d+\.\d+\.processing)?$/;
const clone = <T>(value: T): T => structuredClone(value);
const inside = (child: string, parent: string) =>
  child === parent || child.startsWith(parent + path.sep);

export type LegacySource = { id: string; directory: string };
export type LegacyImportSettings = {
  batchSize?: number;
  concurrency?: number;
  maxFileBytes?: number;
  maxBatchBytes?: number;
  intervalMs?: number;
};
type State =
  "planned" | "running" | "paused" | "completed" | "blocked" | "cancelled";
type Job = {
  id: string;
  sourceId: string;
  revision: number;
  state: State;
  written: number;
  duplicate: number;
  quarantined: number;
  deferred: number;
  bytesRead: number;
  entriesVisited: number;
  inFlight: number;
  rowsWritten: number;
  oldestEventTimeSeen: string | null;
  startedAt: string | null;
  updatedAt: string | null;
  lastError: string | null;
  policyDigest: string;
  sourceDigest: string;
  lastCommand: { requestId: string; digest: string } | null;
};
type Inspection = {
  events: number | null;
  processing: number | null;
  other: number | null;
  entriesVisited: number;
  scanComplete: boolean;
  countKind: "unknown" | "lower-bound" | "observed";
  capturedAt: string | null;
  error: string | null;
};
type SourceRuntime = {
  source: LegacySource;
  canonical?: string;
  device?: number;
  inode?: number;
  loaded: boolean;
  job: Job | null;
  error: string | null;
  inspection: Inspection;
  inventory?: BoundedDirectoryScan;
  replay?: BoundedDirectoryScan;
  sweepRemaining: number;
  inspecting: boolean;
  lockToken?: string;
  nextRun: number;
  active: Promise<void> | null;
  checkpointTail: Promise<void>;
  verification?: BoundedDirectoryScan;
  verifying: boolean;
  verificationRemaining: number;
  quarantinePresent: boolean | null;
  rateSample?: { at: number; acknowledged: number; rows: number };
  drainRate: {
    acknowledgedFilesPerSecond: number | null;
    rowsPerSecond: number | null;
    measuredAt: string | null;
  };
};
export type ImportCommand = {
  action: "start" | "pause" | "resume" | "cancel";
  sourceId: string;
  requestId: string;
  expectedRevision: number;
  confirmSourceClosed?: boolean;
};
export class LegacyImportError extends Error {
  constructor(
    public readonly code: string,
    public readonly status = 409,
  ) {
    super(code);
  }
}
const fault = (code: string, status?: number): never => {
  throw new LegacyImportError(code, status);
};
const digest = (value: unknown) =>
  createHash("sha256").update(JSON.stringify(value)).digest("hex");
const emptyInspection = (): Inspection => ({
  events: null,
  processing: null,
  other: null,
  entriesVisited: 0,
  scanComplete: false,
  countKind: "unknown",
  capturedAt: null,
  error: null,
});

/** Node-local, source-exclusive import. Source paths and file names never appear in API status. */
export class LegacyImportManager {
  private readonly sources = new Map<string, SourceRuntime>();
  private readonly limits: Required<LegacyImportSettings>;
  private timer?: ReturnType<typeof setTimeout>;
  private background: Promise<void> | null = null;
  private commandTail: Promise<unknown> = Promise.resolve();
  private commandBusy = false;
  private closed = false;

  constructor(
    private readonly options: {
      sources: LegacySource[];
      liveDirectory: string;
      instanceId: string;
      settings?: LegacyImportSettings;
      policyDigest: () => string;
      canWrite: () => boolean;
      hasLiveHeadroom: () => boolean;
      write: (
        event: LegacyEvent,
        context: { startedAt: string },
      ) => Promise<LegacyWriteResult>;
    },
  ) {
    if (options.sources.length > 16) fault("too-many-sources", 400);
    for (const source of options.sources) {
      if (
        !ID.test(source.id) ||
        this.sources.has(source.id) ||
        !path.isAbsolute(source.directory)
      )
        fault("invalid-source-configuration", 400);
      this.sources.set(source.id, {
        source: { ...source },
        loaded: false,
        job: null,
        error: null,
        inspection: emptyInspection(),
        sweepRemaining: 0,
        inspecting: false,
        nextRun: 0,
        active: null,
        checkpointTail: Promise.resolve(),
        verifying: false,
        verificationRemaining: 0,
        quarantinePresent: null,
        drainRate: {
          acknowledgedFilesPerSecond: null,
          rowsPerSecond: null,
          measuredAt: null,
        },
      });
    }
    this.limits = {
      batchSize: 128,
      concurrency: 64,
      maxFileBytes: 1024 * 1024,
      maxBatchBytes: 16 * 1024 * 1024,
      intervalMs: 500,
      ...options.settings,
    };
    const bounds = {
      batchSize: [1, 512],
      concurrency: [1, 128],
      maxFileBytes: [1, 16 * 1024 * 1024],
      maxBatchBytes: [1, 64 * 1024 * 1024],
      intervalMs: [100, 60_000],
    };
    for (const [key, [min, max]] of Object.entries(bounds)) {
      const value = this.limits[key as keyof LegacyImportSettings];
      if (!Number.isSafeInteger(value) || value < min || value > max)
        fault("invalid-import-limits", 400);
    }
    if (this.limits.maxBatchBytes < this.limits.maxFileBytes)
      fault("invalid-import-limits", 400);
  }

  status() {
    return [...this.sources.values()].map((runtime) => ({
      sourceId: runtime.source.id,
      loaded: runtime.loaded,
      ownership: runtime.lockToken ? "owned" : "unclaimed",
      error: runtime.error,
      inspection: clone(runtime.inspection),
      quarantinePresent: runtime.quarantinePresent,
      drainRate: clone(runtime.drainRate),
      job: runtime.job ? this.publicJob(runtime.job) : null,
    }));
  }
  private publicJob(job: Job) {
    return clone({
      id: job.id,
      sourceId: job.sourceId,
      revision: job.revision,
      state: job.state,
      written: job.written,
      duplicate: job.duplicate,
      quarantined: job.quarantined,
      deferred: job.deferred,
      bytesRead: job.bytesRead,
      entriesVisited: job.entriesVisited,
      rowsWritten: job.rowsWritten,
      oldestEventTimeSeen: job.oldestEventTimeSeen,
      inFlight: job.inFlight,
      startedAt: job.startedAt,
      updatedAt: job.updatedAt,
      lastError: job.lastError,
    });
  }

  /** Scheduling only; no directory IO on HTTP inspection/status requests. */
  inspect(sourceId: string) {
    const runtime = this.getSource(sourceId);
    if (!runtime.inspecting) {
      runtime.inspection = emptyInspection();
      runtime.inspecting = true;
    }
    return this.status();
  }
  startBackground(): void {
    if (!this.timer && !this.closed) this.schedule();
  }
  private schedule() {
    this.timer = setTimeout(() => {
      this.timer = undefined;
      this.background = this.tick().catch(() => undefined);
      void this.background.finally(() => {
        this.background = null;
        if (!this.closed) this.schedule();
      });
    }, 100);
    this.timer.unref();
  }
  private getSource(id: string) {
    return this.sources.get(id) ?? fault("unknown-source", 404);
  }

  command(command: ImportCommand) {
    const work = this.commandTail.then(async () => {
      this.commandBusy = true;
      try {
        return await this.applyCommand(command);
      } finally {
        this.commandBusy = false;
      }
    });
    this.commandTail = work.catch(() => undefined);
    return work;
  }

  private async applyCommand(command: ImportCommand) {
    if (this.closed) fault("importer-stopping");
    if (
      !ID.test(command.requestId) ||
      !Number.isSafeInteger(command.expectedRevision) ||
      command.expectedRevision < 0
    )
      fault("invalid-command", 400);
    if (!["start", "pause", "resume", "cancel"].includes(command.action))
      fault("invalid-action", 400);
    const runtime = this.getSource(command.sourceId);
    if (command.action !== "pause") await runtime.active;
    await this.load(runtime);
    const commandDigest = digest(command);
    if (runtime.job?.lastCommand?.requestId === command.requestId) {
      if (runtime.job.lastCommand.digest !== commandDigest)
        fault("request-id-conflict");
      return this.publicJob(runtime.job);
    }
    if ((runtime.job?.revision ?? 0) !== command.expectedRevision)
      fault("revision-conflict");
    if (command.action === "start" || command.action === "resume") {
      if (command.confirmSourceClosed !== true)
        fault("source-closure-confirmation-required", 400);
      if (runtime.job?.state === "completed") fault("job-already-completed");
      if (runtime.job?.state === "running") fault("job-already-running");
      if (command.action === "resume" && !runtime.job) fault("job-not-started");
      await this.claim(runtime);
      if ((runtime.job?.revision ?? 0) !== command.expectedRevision)
        fault("revision-conflict");
      if (
        runtime.job &&
        runtime.job.policyDigest !== this.options.policyDigest()
      )
        fault("storage-policy-changed");
      runtime.job ??= {
        id: randomUUID(),
        sourceId: runtime.source.id,
        revision: 0,
        state: "planned",
        written: 0,
        duplicate: 0,
        quarantined: 0,
        deferred: 0,
        bytesRead: 0,
        entriesVisited: 0,
        rowsWritten: 0,
        oldestEventTimeSeen: null,
        inFlight: 0,
        startedAt: new Date().toISOString(),
        updatedAt: null,
        lastError: null,
        policyDigest: this.options.policyDigest(),
        sourceDigest: digest(runtime.canonical),
        lastCommand: null,
      };
      runtime.job.state = "running";
      runtime.job.lastError = null;
      runtime.rateSample = undefined;
      runtime.drainRate = {
        acknowledgedFilesPerSecond: null,
        rowsPerSecond: null,
        measuredAt: null,
      };
      runtime.replay ??= new BoundedDirectoryScan(runtime.canonical!);
    } else {
      if (!runtime.job) fault("job-not-started");
      if (runtime.job!.state === "completed") fault("job-already-completed");
      // Never change a persisted job owned by another runtime.
      await this.claim(runtime);
      if ((runtime.job?.revision ?? 0) !== command.expectedRevision)
        fault("revision-conflict");
      runtime.job!.state = command.action === "pause" ? "paused" : "cancelled";
    }
    runtime.job!.revision++;
    runtime.job!.lastCommand = {
      requestId: command.requestId,
      digest: commandDigest,
    };
    await this.checkpoint(runtime);
    if (command.action === "cancel") await this.release(runtime);
    return this.publicJob(runtime.job!);
  }

  private async validateSource(runtime: SourceRuntime) {
    const canonical = await fs.realpath(runtime.source.directory);
    if (canonical !== path.resolve(runtime.source.directory))
      fault("source-symlink-not-allowed");
    const stat = await fs.lstat(canonical);
    if (!stat.isDirectory()) fault("source-not-directory");
    const live = await fs.realpath(this.options.liveDirectory);
    if (inside(canonical, live) || inside(live, canonical))
      fault("source-overlaps-live-storage");
    for (const other of this.sources.values()) {
      if (other === runtime) continue;
      const otherPath = other.canonical ?? path.resolve(other.source.directory);
      if (inside(canonical, otherPath) || inside(otherPath, canonical))
        fault("source-overlap");
    }
    if (
      runtime.canonical &&
      (runtime.canonical !== canonical ||
        runtime.device !== stat.dev ||
        runtime.inode !== stat.ino)
    )
      fault("source-directory-changed");
    runtime.canonical = canonical;
    runtime.device = stat.dev;
    runtime.inode = stat.ino;
  }
  private statePath(runtime: SourceRuntime, name: string) {
    return path.join(runtime.canonical!, STATE_DIRECTORY, name);
  }

  private async readJson(file: string, maxBytes = 64 * 1024): Promise<any> {
    const handle = await fs.open(
      file,
      constants.O_RDONLY | constants.O_NOFOLLOW,
    );
    try {
      const stat = await handle.stat();
      if (!stat.isFile() || stat.size > maxBytes) fault("invalid-state-file");
      const bytes = Buffer.alloc(maxBytes + 1);
      const read = await handle.read(bytes, 0, bytes.length, 0);
      if (read.bytesRead > maxBytes) fault("invalid-state-file");
      return JSON.parse(bytes.subarray(0, read.bytesRead).toString("utf8"));
    } finally {
      await handle.close();
    }
  }
  private async load(runtime: SourceRuntime) {
    if (runtime.loaded) return;
    try {
      await this.validateSource(runtime);
      const stateDir = this.statePath(runtime, "");
      try {
        if (
          (await fs.realpath(stateDir)) !== stateDir ||
          !(await fs.lstat(stateDir)).isDirectory()
        )
          fault("state-symlink-not-allowed");
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      }
      const state = this.statePath(runtime, "checkpoint.json");
      try {
        const job = await this.readJson(state);
        if (
          !job ||
          job.sourceId !== runtime.source.id ||
          job.sourceDigest !== digest(runtime.canonical) ||
          typeof job.id !== "string" ||
          !/^[0-9a-f-]{36}$/.test(job.id) ||
          !Number.isSafeInteger(job.revision) ||
          job.revision < 0 ||
          typeof job.policyDigest !== "string" ||
          job.policyDigest.length > 128 ||
          typeof job.startedAt !== "string" ||
          !Number.isFinite(Date.parse(job.startedAt)) ||
          (job.lastError !== null &&
            (typeof job.lastError !== "string" ||
              !/^[a-z][a-z0-9-]{0,80}$/.test(job.lastError))) ||
          ![
            "planned",
            "running",
            "paused",
            "completed",
            "blocked",
            "cancelled",
          ].includes(job.state) ||
          [
            "written",
            "duplicate",
            "quarantined",
            "deferred",
            "bytesRead",
            "entriesVisited",
            "rowsWritten",
          ].some((key) => !Number.isSafeInteger(job[key]) || job[key] < 0)
        )
          fault("invalid-checkpoint");
        runtime.job = job;
        if (job.state === "running") {
          job.state = "paused";
          job.lastError = "restart-review-required";
        }
        job.inFlight = 0;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      }
      runtime.loaded = true;
      runtime.error = null;
    } catch (error) {
      runtime.error = this.errorCode(error);
      throw new LegacyImportError(runtime.error);
    }
  }

  private async claim(runtime: SourceRuntime) {
    if (runtime.lockToken) {
      await this.assertOwner(runtime);
      return;
    }
    await this.validateSource(runtime);
    const stateDir = this.statePath(runtime, "");
    await fs.mkdir(stateDir, { mode: 0o700 }).catch((error) => {
      if (error.code !== "EEXIST") throw error;
    });
    if ((await fs.realpath(stateDir)) !== stateDir)
      fault("state-symlink-not-allowed");
    const lock = this.statePath(runtime, "lock");
    let created = false;
    try {
      await fs.mkdir(lock, { mode: 0o700 });
      created = true;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    }
    const token = randomUUID();
    const owner = {
      token,
      hostname: hostname(),
      pid: process.pid,
      instanceId: this.options.instanceId,
    };
    if (!created) {
      if ((await fs.realpath(lock)) !== lock) fault("lock-symlink-not-allowed");
      // An atomic recovery mutex prevents two claimants from replacing a dead owner together.
      const recovery = path.join(lock, "recovery");
      await fs.mkdir(recovery).catch(() => fault("source-already-claimed"));
      try {
        const previous = await this.readJson(path.join(lock, "owner.json"));
        if (
          previous.hostname !== hostname() ||
          !Number.isSafeInteger(previous.pid) ||
          previous.pid <= 0
        )
          fault("source-owner-review-required");
        try {
          process.kill(previous.pid, 0);
          fault("source-already-claimed");
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
        }
        await this.atomicJson(path.join(lock, "owner.json"), owner);
      } finally {
        await fs.rmdir(recovery);
      }
    } else {
      await this.atomicJson(path.join(lock, "owner.json"), owner);
    }
    runtime.lockToken = token;
    // Load the latest checkpoint only after exclusivity; a peer may have advanced it.
    runtime.loaded = false;
    await this.load(runtime);
  }
  private async assertOwner(runtime: SourceRuntime) {
    const owner = await this.readJson(
      this.statePath(runtime, "lock/owner.json"),
    );
    if (
      owner.token !== runtime.lockToken ||
      owner.instanceId !== this.options.instanceId
    )
      fault("source-ownership-lost");
  }
  private async release(runtime: SourceRuntime) {
    if (!runtime.lockToken) return;
    await this.assertOwner(runtime);
    await fs.unlink(this.statePath(runtime, "lock/owner.json"));
    await fs.rmdir(this.statePath(runtime, "lock"));
    runtime.lockToken = undefined;
  }
  private async atomicJson(file: string, value: unknown) {
    const temporary = `${file}.${randomUUID()}.tmp`;
    const handle = await fs.open(temporary, "wx", 0o600);
    try {
      await handle.writeFile(JSON.stringify(value));
      await handle.sync();
    } finally {
      await handle.close();
    }
    try {
      await fs.rename(temporary, file);
    } catch (error) {
      await fs.unlink(temporary).catch(() => undefined);
      throw error;
    }
    const directory = await fs.open(path.dirname(file), "r");
    try {
      await directory.sync();
    } finally {
      await directory.close();
    }
  }
  private async checkpoint(runtime: SourceRuntime) {
    const work = runtime.checkpointTail.then(async () => {
      await this.assertOwner(runtime);
      runtime.job!.updatedAt = new Date().toISOString();
      await this.atomicJson(
        this.statePath(runtime, "checkpoint.json"),
        runtime.job,
      );
    });
    runtime.checkpointTail = work.catch(() => undefined);
    await work;
  }
  private errorCode(error: unknown) {
    return error instanceof LegacyImportError
      ? error.code
      : "source-io-unavailable";
  }

  /** Public for deterministic bounded acceptance; status itself never invokes this. */
  async tick(): Promise<void> {
    if (this.closed || this.commandBusy) return;
    for (const runtime of this.sources.values()) {
      if (this.closed || this.commandBusy) break;
      if (runtime.active) continue;
      runtime.active = this.work(runtime);
      try {
        await runtime.active;
      } finally {
        runtime.active = null;
      }
    }
  }
  private async work(runtime: SourceRuntime) {
    if (runtime.error && Date.now() < runtime.nextRun) return;
    try {
      if (!runtime.loaded) await this.load(runtime);
      runtime.error = null;
      if (runtime.inspecting) await this.inspectPass(runtime);
      if (
        runtime.job?.state === "running" &&
        Date.now() >= runtime.nextRun &&
        this.options.canWrite() &&
        this.options.hasLiveHeadroom() &&
        !this.commandBusy &&
        !this.closed
      ) {
        await this.replayPass(runtime);
      }
    } catch (error) {
      runtime.error = this.errorCode(error);
      runtime.inspection.error = runtime.error;
      runtime.inspection.countKind = "unknown";
      runtime.inspection.scanComplete = false;
      if (runtime.job?.state === "running") {
        runtime.job.state = "blocked";
        runtime.job.lastError = runtime.error;
        if (runtime.lockToken)
          await this.checkpoint(runtime).catch(() => undefined);
      }
      runtime.nextRun = Date.now() + 5000;
    }
  }
  private async inspectPass(runtime: SourceRuntime) {
    await this.validateSource(runtime);
    if (runtime.inspection.entriesVisited === 0 && runtime.inventory) {
      await runtime.inventory.close();
      runtime.inventory = undefined;
    }
    runtime.inventory ??= new BoundedDirectoryScan(runtime.canonical!);
    const counts = runtime.inspection;
    counts.events ??= 0;
    counts.processing ??= 0;
    counts.other ??= 0;
    const pass = await runtime.inventory.pass((entry) => {
      if (entry.name === STATE_DIRECTORY && entry.isDirectory()) return;
      if (entry.isFile() && entry.name.endsWith(".event")) counts.events!++;
      else if (
        entry.isFile() &&
        EVENT.test(entry.name) &&
        entry.name.endsWith(".processing")
      )
        counts.processing!++;
      else counts.other!++;
    });
    counts.entriesVisited += pass.entriesVisited;
    counts.scanComplete = pass.complete;
    counts.countKind = pass.complete ? "observed" : "lower-bound";
    counts.capturedAt = new Date().toISOString();
    counts.error = null;
    if (pass.complete) runtime.inspecting = false;
  }

  private async replayPass(runtime: SourceRuntime) {
    await this.validateSource(runtime);
    await this.assertOwner(runtime);
    const job = runtime.job!;
    if (job.policyDigest !== this.options.policyDigest())
      fault("storage-policy-changed");
    if (runtime.verifying) {
      await this.verifyEmptyPass(runtime);
      await this.finishPass(runtime);
      return;
    }
    const selected: {
      name: string;
      size: number;
      inode: number;
      modified: number;
    }[] = [];
    let bytes = 0;
    const pass = await runtime.replay!.pass(
      async (entry) => {
        if (entry.name === STATE_DIRECTORY && entry.isDirectory()) return;
        if (!entry.isFile()) {
          runtime.sweepRemaining++;
          job.lastError = "source-nonregular-entry";
          return;
        }
        const file = path.join(runtime.canonical!, entry.name);
        const stat = await fs.lstat(file).catch((error) => {
          if (error.code === "ENOENT") return null;
          throw error;
        });
        if (!stat) return;
        if (!stat.isFile() || stat.nlink !== 1) {
          runtime.sweepRemaining++;
          job.lastError = "source-nonregular-entry";
          return;
        }
        if (!EVENT.test(entry.name) || stat.size > this.limits.maxFileBytes) {
          await this.quarantine(
            runtime,
            entry.name,
            EVENT.test(entry.name) ? "file-too-large" : "unknown-file",
          );
          job.quarantined++;
          return;
        }
        selected.push({
          name: entry.name,
          size: stat.size,
          inode: stat.ino,
          modified: stat.mtimeMs,
        });
        bytes += stat.size;
        return (
          selected.length < this.limits.batchSize &&
          bytes <= this.limits.maxBatchBytes - this.limits.maxFileBytes
        );
      },
      undefined,
      () => this.closed || this.commandBusy || !this.options.hasLiveHeadroom(),
    );
    job.entriesVisited += pass.entriesVisited;
    for (
      let offset = 0;
      offset < selected.length;
      offset += this.limits.concurrency
    ) {
      const chunk = selected.slice(offset, offset + this.limits.concurrency);
      if (
        this.closed ||
        this.commandBusy ||
        !this.options.hasLiveHeadroom() ||
        !this.options.canWrite()
      ) {
        runtime.sweepRemaining += selected.length - offset;
        break;
      }
      await Promise.all(
        chunk.map(async (entry) => {
          job.inFlight++;
          try {
            const file = path.join(runtime.canonical!, entry.name);
            const handle = await fs.open(
              file,
              constants.O_RDONLY | constants.O_NOFOLLOW,
            );
            let event: LegacyEvent | null = null;
            let invalid = false;
            try {
              const stat = await handle.stat();
              if (
                !stat.isFile() ||
                stat.ino !== entry.inode ||
                stat.size !== entry.size ||
                stat.mtimeMs !== entry.modified
              )
                fault("source-file-changed");
              const buffer = Buffer.alloc(entry.size + 1);
              const read = await handle.read(buffer, 0, buffer.length, 0);
              if (read.bytesRead !== entry.size) fault("source-file-changed");
              job.bytesRead += read.bytesRead;
              try {
                const parsed = JSON.parse(
                  buffer.subarray(0, read.bytesRead).toString("utf8"),
                );
                if (
                  !parsed ||
                  typeof parsed.topic !== "string" ||
                  !parsed.topic ||
                  parsed.message === undefined
                )
                  invalid = true;
                else event = parsed;
              } catch {
                invalid = true;
              }
            } finally {
              await handle.close();
            }
            const result = invalid
              ? ({
                  outcome: "quarantined",
                  reason: "invalid-envelope",
                } as const)
              : await this.options.write(event!, { startedAt: job.startedAt! });
            await this.assertOwner(runtime);
            await this.validateSource(runtime);
            const after = await fs.lstat(file);
            if (
              after.ino !== entry.inode ||
              after.size !== entry.size ||
              after.mtimeMs !== entry.modified
            )
              fault("source-file-changed");
            if (
              result.outcome === "written" ||
              result.outcome === "duplicate"
            ) {
              await fs.unlink(file);
              job[result.outcome]++;
              if (result.outcome === "written") {
                job.rowsWritten += result.rows ?? 0;
                if (
                  result.oldestEventTime &&
                  (!job.oldestEventTimeSeen ||
                    result.oldestEventTime < job.oldestEventTimeSeen)
                )
                  job.oldestEventTimeSeen = result.oldestEventTime;
              }
            } else if (result.outcome === "quarantined") {
              await this.quarantine(runtime, entry.name, result.reason);
              job.quarantined++;
            } else {
              job.deferred++;
              runtime.sweepRemaining++;
              job.lastError = result.reason;
              runtime.nextRun = Date.now() + 5000;
            }
          } catch (error) {
            runtime.sweepRemaining++;
            job.lastError = this.errorCode(error);
            runtime.nextRun = Date.now() + 5000;
          } finally {
            job.inFlight--;
          }
        }),
      );
    }
    if (
      pass.complete &&
      job.state === "running" &&
      !this.closed &&
      !this.commandBusy
    ) {
      if (runtime.sweepRemaining === 0) {
        // Deletions during replay can disturb filesystem iteration. Confirm with
        // a fresh traversal that does not mutate the source before reporting done.
        runtime.verifying = true;
        runtime.verification = new BoundedDirectoryScan(runtime.canonical!);
        await this.verifyEmptyPass(runtime);
      } else if (job.lastError === "source-nonregular-entry") {
        job.state = "blocked";
      }
      runtime.sweepRemaining = 0;
    }
    await this.finishPass(runtime);
  }
  private async verifyEmptyPass(runtime: SourceRuntime) {
    const job = runtime.job!;
    const pass = await runtime.verification!.pass(
      (entry) => {
        if (entry.name !== STATE_DIRECTORY || !entry.isDirectory())
          runtime.verificationRemaining++;
      },
      undefined,
      () => this.closed || this.commandBusy,
    );
    if (pass.complete) {
      runtime.verifying = false;
      await runtime.verification!.close();
      runtime.verification = undefined;
      if (runtime.verificationRemaining === 0) {
        let hasQuarantine = false;
        try {
          const directory = this.statePath(runtime, "quarantine");
          if ((await fs.realpath(directory)) !== directory)
            fault("quarantine-symlink-not-allowed");
          const cursor = await fs.opendir(directory, { bufferSize: 1 });
          try {
            hasQuarantine = !!(await cursor.read());
          } finally {
            await cursor.close();
          }
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
        }
        runtime.quarantinePresent = hasQuarantine;
        job.state = hasQuarantine || job.quarantined ? "blocked" : "completed";
        job.lastError =
          job.state === "blocked" ? "quarantine-review-required" : null;
      } else {
        await runtime.replay!.close();
        runtime.replay = new BoundedDirectoryScan(runtime.canonical!);
      }
      runtime.verificationRemaining = 0;
    }
  }
  private async finishPass(runtime: SourceRuntime) {
    const job = runtime.job!;
    const sample = {
      at: Date.now(),
      acknowledged: job.written + job.duplicate,
      rows: job.rowsWritten,
    };
    if (runtime.rateSample && sample.at > runtime.rateSample.at) {
      const seconds = (sample.at - runtime.rateSample.at) / 1000;
      runtime.drainRate = {
        acknowledgedFilesPerSecond:
          (sample.acknowledged - runtime.rateSample.acknowledged) / seconds,
        rowsPerSecond: (sample.rows - runtime.rateSample.rows) / seconds,
        measuredAt: new Date(sample.at).toISOString(),
      };
    }
    runtime.rateSample = sample;
    await this.checkpoint(runtime);
    runtime.nextRun = Math.max(
      runtime.nextRun,
      Date.now() + this.limits.intervalMs,
    );
    if (job.state === "completed") await this.release(runtime);
  }
  private async quarantine(
    runtime: SourceRuntime,
    name: string,
    reason: string,
  ) {
    const directory = this.statePath(runtime, "quarantine");
    await fs.mkdir(directory, { mode: 0o700 }).catch((error) => {
      if (error.code !== "EEXIST") throw error;
    });
    if ((await fs.realpath(directory)) !== directory)
      fault("quarantine-symlink-not-allowed");
    const id = randomUUID();
    await this.atomicJson(path.join(directory, `${id}.json`), {
      reason,
      originalName: name,
    });
    await fs.rename(
      path.join(runtime.canonical!, name),
      path.join(directory, `${id}.preserved`),
    );
    runtime.quarantinePresent = true;
  }
  async close() {
    this.closed = true;
    if (this.timer) clearTimeout(this.timer);
    await this.commandTail;
    await this.background;
    for (const runtime of this.sources.values()) {
      await runtime.active;
      if (runtime.lockToken && runtime.job) {
        if (runtime.job.state === "running") {
          runtime.job.state = "paused";
          runtime.job.lastError = "shutdown-review-required";
        }
        await this.checkpoint(runtime);
      }
      if (runtime.lockToken) await this.release(runtime);
      await runtime.inventory?.close();
      await runtime.replay?.close();
      await runtime.verification?.close();
    }
  }
}
