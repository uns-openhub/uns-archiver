import { createHash } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import { closeSync, constants, fstatSync, openSync } from "node:fs";
import {
  assertHistoricalImportSettings,
  ImportError,
  historicalSourceIdentity,
  isoMicros,
  MAX_PAGE_ROWS,
  packetForHistoricalRow,
  questDbHistoricalReader,
  utcMicros,
  type HistoricalImportSettings,
  type HistoricalReader,
  type ImportCursor,
  type ImportMapping,
  type ImportProfile,
  type SourceRow,
} from "./historical-import-source.js";
import {
  inspectHistoricalTargets,
  verifyHistoricalVisibility,
  type HistoricalTarget,
} from "./historical-import-target.js";

type RangeRequest = { profileId: string; from: string; to: string };
type StartRequest = RangeRequest & { requestId: string; previewDigest: string };
type Job = {
  id: string;
  actor: string;
  requestHash: string;
  range: RangeRequest;
  contract: string;
  sourceSeal: string;
  state: "running" | "blocked" | "completed";
  cursor: ImportCursor | null;
  written: number;
  skippedAfterCutoff: number;
  reason: string | null;
  receipts: string[];
  sourcesRecorded?: true;
};
type Prepared = {
  profile: ImportProfile;
  reader: HistoricalReader;
  from: string;
  to: string;
  targets: HistoricalTarget[];
  contract: string;
};
type HistoricalSource = {
  topic: string;
  tableName: string;
  tablePrefix: string;
  dataGroup: string | null;
  suffix: "_data" | "_table";
  identity: string;
};
type Dependencies = {
  directoryFile: string;
  resolveTarget: (
    mapping: ImportMapping,
    profile: ImportProfile,
  ) => HistoricalTarget;
  readTarget: (sql: string) => Promise<unknown[][]>;
  canWrite: () => boolean;
  write: (
    mapping: ImportMapping,
    target: HistoricalTarget,
    record: ReturnType<typeof packetForHistoricalRow>,
    row: SourceRow,
  ) => Promise<void>;
  acknowledge: (id: string) => void;
  onSourcesChanged?: () => void;
  openReader?: (connectionString: string) => HistoricalReader;
};

const digest = (value: unknown) =>
  createHash("sha256").update(JSON.stringify(value)).digest("hex");
const maxRange = 31n * 86400n * 1000000n;

export class HistoricalImport {
  private readonly db: DatabaseSync;
  private readonly profiles = new Map<string, ImportProfile>();
  private readonly unrecordedSources: string[];
  private readonly readers = new Map<
    string,
    { identity: string; reader: HistoricalReader }
  >();
  private timer?: NodeJS.Timeout;
  private inFlight?: Promise<void>;
  private stopped = false;
  private faulted = false;
  private startPending = false;
  private previewWork?: ReturnType<HistoricalImport["buildPreview"]>;
  private closing?: Promise<void>;
  private admittedContract?: string;
  private retryDelay = 1000;

  constructor(
    settings: HistoricalImportSettings | undefined,
    private readonly dependencies: Dependencies,
  ) {
    if (settings) assertHistoricalImportSettings(settings);
    for (const source of settings?.sources ?? []) {
      if (typeof source.connectionString !== "string")
        throw new Error("Historical source credential not resolved");
      const reader = (dependencies.openReader ?? questDbHistoricalReader)(
        source.connectionString,
      );
      this.readers.set(source.id, {
        identity: historicalSourceIdentity(source.connectionString),
        reader,
      });
    }
    for (const profile of settings?.profiles ?? [])
      this.profiles.set(profile.id, profile);
    const file = openSync(
      dependencies.directoryFile,
      constants.O_CREAT | constants.O_NOFOLLOW | constants.O_RDWR,
      0o600,
    );
    try {
      const stat = fstatSync(file);
      if (
        !stat.isFile() ||
        stat.nlink !== 1 ||
        stat.uid !== process.getuid?.() ||
        stat.mode & 0o077
      ) {
        throw new Error("Unsafe historical import job store");
      }
    } finally {
      closeSync(file);
    }
    this.db = new DatabaseSync(dependencies.directoryFile);
    this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
      CREATE TABLE IF NOT EXISTS imports(id TEXT PRIMARY KEY, state TEXT NOT NULL, job TEXT NOT NULL) STRICT;
      CREATE TABLE IF NOT EXISTS history_sources(
        topic TEXT NOT NULL, table_name TEXT NOT NULL, identity TEXT NOT NULL, mapping TEXT NOT NULL,
        PRIMARY KEY(topic, table_name, identity)
      ) STRICT`);
    this.unrecordedSources = settings
      ? this.db
          .prepare(
            `SELECT id FROM imports WHERE state = 'completed'
        AND json_extract(job, '$.written') > 0
        AND json_type(job, '$.sourcesRecorded') IS NULL ORDER BY rowid`,
          )
          .all()
          .map((row) => String(row.id))
      : [];
  }

  private readJob(id: string): Job | undefined {
    const row = this.db.prepare("SELECT job FROM imports WHERE id=?").get(id);
    return row ? (JSON.parse(String(row.job)) as Job) : undefined;
  }

  private active(): Job | undefined {
    const row = this.db
      .prepare(
        "SELECT job FROM imports WHERE state <> 'completed' ORDER BY rowid LIMIT 1",
      )
      .get();
    return row ? (JSON.parse(String(row.job)) as Job) : undefined;
  }

  private store(job: Job) {
    this.db
      .prepare(
        "INSERT INTO imports VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET state=excluded.state, job=excluded.job",
      )
      .run(job.id, job.state, JSON.stringify(job));
  }

  publishedSources(): HistoricalSource[] {
    return this.db
      .prepare(
        "SELECT mapping FROM history_sources ORDER BY topic, table_name, identity",
      )
      .all()
      .map((row) => JSON.parse(String(row.mapping)) as HistoricalSource);
  }

  private complete(job: Job, plan: Prepared, verified: HistoricalTarget[]) {
    const writeSource = this.db.prepare(
      "INSERT OR IGNORE INTO history_sources(topic, table_name, identity, mapping) VALUES (?, ?, ?, ?)",
    );
    let changed = false;
    this.db.exec("BEGIN");
    try {
      for (const target of verified) {
        const mapping = plan.profile.mappings[plan.targets.indexOf(target)]!;
        const source: HistoricalSource = {
          topic: target.topic,
          tableName: target.table,
          tablePrefix: target.prefix,
          dataGroup: mapping.dataGroup ?? null,
          suffix: target.kind === "data" ? "_data" : "_table",
          identity: target.identity,
        };
        const result = writeSource.run(
          source.topic,
          source.tableName,
          source.identity,
          JSON.stringify(source),
        );
        changed ||= result.changes !== 0;
      }
      this.store({
        ...job,
        state: "completed",
        reason: null,
        sourcesRecorded: true,
      });
      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
    return changed;
  }

  private statusOf(job: Job) {
    return {
      id: job.id,
      profileId: job.range.profileId,
      from: job.range.from,
      to: job.range.to,
      state: job.state,
      written: job.written,
      skippedAfterCutoff: job.skippedAfterCutoff,
      lastWrittenAt: job.cursor ? isoMicros(job.cursor.time) : null,
      reason: job.reason,
    };
  }

  status() {
    const rows = this.db
      .prepare("SELECT job FROM imports ORDER BY rowid DESC LIMIT 10")
      .all();
    return {
      healthy: !this.faulted,
      profiles: [...this.profiles.values()].map((p) => ({
        id: p.id,
        sourceId: p.source,
        mappings: p.mappings.length,
      })),
      jobs: rows.map((row) =>
        this.statusOf(JSON.parse(String(row.job)) as Job),
      ),
    };
  }

  get activeWork() {
    return this.inFlight ? 1 : 0;
  }

  assertHandoverReady() {
    if (this.faulted || this.startPending || this.active())
      throw new Error(
        "Historical import must complete before Archiver handover",
      );
  }

  private prepare(input: RangeRequest): Prepared {
    const profile = this.profiles.get(input.profileId);
    if (!profile) throw new ImportError(404, "historical-profile-not-found");
    const from = utcMicros(input.from);
    const to = utcMicros(input.to);
    if (
      BigInt(from) < 0n ||
      BigInt(from) >= BigInt(to) ||
      BigInt(to) - BigInt(from) > maxRange ||
      BigInt(to) > BigInt(Date.now()) * 1000n
    ) {
      throw new ImportError(400, "invalid-historical-range");
    }
    const source = this.readers.get(profile.source)!;
    const targets = profile.mappings.map((mapping) => ({
      ...this.dependencies.resolveTarget(mapping, profile),
      before: mapping.before ? utcMicros(mapping.before) : undefined,
    }));
    return {
      profile,
      reader: source.reader,
      from,
      to,
      targets,
      contract: digest([source.identity, profile, targets]),
    };
  }

  async preview(input: RangeRequest) {
    return (await this.review(input)).result;
  }

  private async review(input: RangeRequest) {
    if (this.stopped || this.previewWork)
      throw new ImportError(409, "historical-preview-busy");
    this.previewWork = this.buildPreview(input);
    try {
      return await this.previewWork;
    } finally {
      this.previewWork = undefined;
    }
  }

  private async buildPreview(input: RangeRequest) {
    const deadline = performance.now() + 8500;
    const checkTime = () => {
      if (this.stopped || performance.now() > deadline)
        throw new ImportError(503, "historical-preview-timeout");
    };
    const plan = this.prepare(input);
    const { rows: sample, seal } = await plan.reader.read(
      plan.profile,
      plan.from,
      plan.to,
      null,
      16,
    );
    const accepted = this.mapRows(plan, sample, "preview");
    const inspected = await inspectHistoricalTargets(
      (sql) => {
        checkTime();
        return this.dependencies.readTarget(sql);
      },
      plan.targets,
      plan.from,
      plan.to,
    );
    checkTime();
    return {
      plan,
      seal,
      result: {
        profileId: plan.profile.id,
        from: isoMicros(plan.from),
        to: isoMicros(plan.to),
        targets: inspected.map(
          ({ prefix: _prefix, identity: _identity, ...target }) => target,
        ),
        sampleRows: sample.length,
        eligibleSampleRows: accepted.length,
        canStart:
          sample.length > 0 &&
          inspected.every((target) => !target.overlap) &&
          inspected.some(
            (target) => BigInt(utcMicros(target.before)) > BigInt(plan.from),
          ),
        previewDigest: digest([
          plan.contract,
          plan.from,
          plan.to,
          inspected,
          seal,
          sample,
        ]),
      },
    };
  }

  async start(input: StartRequest, actor: string) {
    const { requestId, previewDigest, ...range } = input;
    if (this.stopped) throw new ImportError(409, "historical-import-not-ready");
    const requestHash = digest([input, actor]);
    const previous = this.readJob(requestId);
    if (previous) {
      if (previous.requestHash !== requestHash)
        throw new ImportError(409, "historical-request-id-conflict");
      return this.statusOf(previous);
    }
    if (
      this.stopped ||
      this.faulted ||
      this.startPending ||
      this.active() ||
      !this.dependencies.canWrite()
    ) {
      throw new ImportError(409, "historical-import-not-ready");
    }
    this.startPending = true;
    try {
      const { result: preview, plan, seal } = await this.review(range);
      if (!preview.canStart || preview.previewDigest !== previewDigest)
        throw new ImportError(409, "historical-preview-stale-or-overlapping");
      if (this.stopped || !this.dependencies.canWrite())
        throw new ImportError(409, "historical-import-not-ready");
      if (this.prepare(range).contract !== plan.contract)
        throw new ImportError(409, "historical-contract-changed");
      const overlapping = this.db
        .prepare(
          `SELECT 1 FROM imports WHERE
        json_extract(job, '$.range.profileId')=?
        AND json_extract(job, '$.range.from') < ? AND json_extract(job, '$.range.to') > ? LIMIT 1`,
        )
        .get(range.profileId, isoMicros(plan.to), isoMicros(plan.from));
      if (overlapping)
        throw new ImportError(409, "historical-job-range-overlap");
      const job: Job = {
        id: requestId,
        actor,
        requestHash,
        range: {
          profileId: range.profileId,
          from: isoMicros(plan.from),
          to: isoMicros(plan.to),
        },
        contract: plan.contract,
        sourceSeal: seal,
        state: "running",
        cursor: null,
        written: 0,
        skippedAfterCutoff: 0,
        reason: null,
        receipts: [],
      };
      this.store(job);
      this.kick();
      return this.statusOf(job);
    } finally {
      this.startPending = false;
    }
  }

  private mappingFor(plan: Prepared, row: SourceRow) {
    const mapping = plan.profile.mappings.find(
      (item) => item.sourceSeries === row.series,
    );
    if (!mapping)
      throw new ImportError(422, "unmapped-historical-source-series");
    return mapping;
  }

  private mapRows(plan: Prepared, rows: SourceRow[], jobId: string) {
    return rows.flatMap((row) => {
      const mapping = this.mappingFor(plan, row);
      const target = plan.targets[plan.profile.mappings.indexOf(mapping)];
      if (target.before && BigInt(row.cursor.time) >= BigInt(target.before))
        return [];
      return [
        {
          row,
          mapping,
          target,
          record: packetForHistoricalRow(plan.profile, mapping, row, jobId),
        },
      ];
    });
  }

  kick(delay = 0) {
    if (
      this.stopped ||
      this.faulted ||
      this.timer ||
      this.inFlight ||
      (this.active()?.state !== "running" && !this.unrecordedSources.length)
    )
      return;
    this.timer = setTimeout(() => {
      this.timer = undefined;
      if (this.stopped) return;
      this.inFlight = this.step()
        .catch(() => {
          this.faulted = true;
        })
        .finally(() => {
          this.inFlight = undefined;
          this.kick(this.retryDelay);
        });
    }, delay);
    this.timer.unref();
  }

  private clearReceipts(job: Job) {
    for (const receipt of job.receipts) this.dependencies.acknowledge(receipt);
    if (job.receipts.length) {
      job.receipts = [];
      this.store(job);
    }
  }

  private async recoverCompletedSources() {
    if (!this.unrecordedSources.length || !this.dependencies.canWrite()) return;
    const id = this.unrecordedSources.shift()!;
    const job = this.readJob(id)!;
    try {
      const plan = this.prepare(job.range);
      // A completed receipt can only authorize the originally reviewed
      // configuration. Verify the rows on the same destination again.
      if (job.contract !== plan.contract) return;
      const verified = await verifyHistoricalVisibility(
        this.dependencies.readTarget,
        plan.targets,
        plan.from,
        plan.to,
        job.id,
        job.written,
      );
      if (this.complete(job, plan, verified))
        this.dependencies.onSourcesChanged?.();
    } catch (error) {
      if (error instanceof ImportError && error.status < 500) return;
      this.unrecordedSources.push(id);
      this.retryDelay = 5000;
    }
  }

  private async step() {
    const job = this.active();
    if (!job || job.state !== "running") {
      await this.recoverCompletedSources();
      return;
    }
    let sourcesChanged = false;
    try {
      this.retryDelay = 1000;
      if (!this.dependencies.canWrite()) return;
      this.clearReceipts(job);
      const plan = this.prepare(job.range);
      if (job.contract !== plan.contract)
        throw new ImportError(409, "historical-contract-changed");
      if (this.admittedContract !== job.id) {
        const targets = await inspectHistoricalTargets(
          this.dependencies.readTarget,
          plan.targets,
          plan.from,
          plan.to,
          job.id,
        );
        if (targets.some((item) => item.overlap))
          throw new ImportError(409, "historical-target-overlap");
        this.admittedContract = job.id;
      }
      const { rows, seal } = await plan.reader.read(
        plan.profile,
        plan.from,
        plan.to,
        job.cursor,
        MAX_PAGE_ROWS,
      );
      if (job.sourceSeal !== seal)
        throw new ImportError(409, "historical-source-changed");
      if (!rows.length) {
        const verified = await verifyHistoricalVisibility(
          this.dependencies.readTarget,
          plan.targets,
          plan.from,
          plan.to,
          job.id,
          job.written,
        );
        sourcesChanged = this.complete(job, plan, verified);
        return;
      }

      // Validate the entire page before submitting any write. Acknowledgements are
      // retained until the corresponding source cursor has been committed to disk.
      const page = this.mapRows(plan, rows, job.id);
      for (let offset = 0; offset < page.length; offset += 8) {
        if (this.stopped || !this.dependencies.canWrite()) return;
        const results = await Promise.allSettled(
          page
            .slice(offset, offset + 8)
            .map((item) =>
              this.dependencies.write(
                item.mapping,
                item.target,
                item.record,
                item.row,
              ),
            ),
        );
        const failure = results.find((result) => result.status === "rejected");
        if (failure?.status === "rejected") throw failure.reason;
      }
      const checkpoint: Job = {
        ...job,
        cursor: rows.at(-1)!.cursor,
        written: job.written + page.length,
        reason: null,
        skippedAfterCutoff: job.skippedAfterCutoff + rows.length - page.length,
        receipts: page.map((item) => item.record.eventId),
      };
      this.store(checkpoint);
      this.retryDelay = 100;
    } catch (error) {
      const permanent = error instanceof ImportError && error.status < 500;
      this.store({
        ...job,
        state: permanent ? "blocked" : "running",
        reason:
          error instanceof ImportError ? error.code : "historical-io-retry",
      });
      this.admittedContract = undefined;
      this.retryDelay = 5000;
    } finally {
      if (sourcesChanged) this.dependencies.onSourcesChanged?.();
    }
  }

  close() {
    this.stopped = true;
    clearTimeout(this.timer);
    this.closing ??= Promise.allSettled([this.inFlight, this.previewWork]).then(
      () => {
        this.db.close();
      },
    );
    return this.closing;
  }
}
