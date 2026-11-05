import { DatabaseSync } from "node:sqlite";
import { createHash } from "node:crypto";
import {
  closeSync,
  existsSync,
  fsyncSync,
  lstatSync,
  mkdirSync,
  openSync,
  readFileSync,
  renameSync,
  statfsSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import { z } from "zod";
import { TopicMatcher } from "./topic-matcher.js";

const table = z
  .string()
  .min(1)
  .max(255)
  .refine((value) => !/[\x00-\x1f]/.test(value));
const selectorSchema = z
  .object({
    tables: z.array(table).min(1).max(64).optional(),
    topicFilter: z
      .string()
      .min(1)
      .max(2048)
      .refine((value) =>
        value
          .split("/")
          .every(
            (part, i, all) =>
              (!part.includes("#") || (part === "#" && i === all.length - 1)) &&
              (!part.includes("+") || part === "+"),
          ),
      )
      .optional(),
  })
  .strict()
  .refine(
    (value) => Boolean(value.tables) !== Boolean(value.topicFilter),
    "Choose tables or topicFilter",
  );
export const targetCommandSchema = z.discriminatedUnion("action", [
  z.object({ action: z.literal("preview"), selector: selectorSchema }).strict(),
  z
    .object({
      action: z.literal("pause"),
      selector: selectorSchema,
      requestId: z.uuid(),
      expectedRevision: z.number().int().nonnegative(),
      reason: z.string().trim().min(1).max(240),
    })
    .strict(),
  z
    .object({
      action: z.literal("resume"),
      pauseId: z.uuid(),
      requestId: z.uuid(),
      expectedRevision: z.number().int().nonnegative(),
      reason: z.string().trim().min(1).max(240),
      confirmUnknownWriteOutcome: z.boolean().optional(),
    })
    .strict(),
]);
const ruleSchema = z
  .object({
    id: z.uuid(),
    selector: selectorSchema,
    reason: z.string().max(240),
    actor: z.string().min(1).max(200),
    at: z.string().datetime(),
    tables: z.array(table).max(10000),
  })
  .strict();
export const policySchema = z
  .object({
    protocol: z.literal(1),
    revision: z.number().int().nonnegative(),
    rules: z.array(ruleSchema).max(64),
  })
  .strict();
type Selector = z.infer<typeof selectorSchema>;
type Policy = z.infer<typeof policySchema>;
export type JournalEntry<T> = {
  id: string;
  table: string;
  topic: string;
  payload: T;
  legacyKey?: string;
};
type Row = {
  seq: number;
  id: string;
  target: string;
  topic: string;
  payload: string;
  legacy_key: string | null;
  state: string;
  attempts: number;
  bytes: number;
};

export class TargetControlError extends Error {
  constructor(
    public readonly status: number,
    message: string,
  ) {
    super(message);
  }
}
export class TargetWriteDeferred extends Error {}
export class TargetStorageError extends Error {
  constructor(cause: unknown) {
    super("Durable target storage failed; acceptance not confirmed", { cause });
  }
}

/** Archiver-owned writes only. An ILP acknowledgement is not a global database lock. */
export class TargetJournal<T> {
  private readonly db!: DatabaseSync;
  private readonly lease: DatabaseSync;
  private policy: Policy = { protocol: 1, revision: 0, rules: [] };
  private readonly active = new Map<string, Set<Promise<void>>>();
  private readonly jobs = new Map<string, Promise<void>>();
  private replaying?: Promise<void>;
  private closing?: Promise<void>;
  private closed = false;
  private closedDrained = false;
  private globalPaused = false;
  private storageError: string | null = null;
  private timer?: ReturnType<typeof setInterval>;
  private cursor = "";
  private policyBusy = false;
  private readonly maxBytes: number;
  private readonly reserveBytes: number;
  private readonly maxPendingPerTarget: number;
  private activeBytes = 0;

  constructor(
    private readonly options: {
      directory: string;
      policyFile: string;
      write: (entry: JournalEntry<T>) => Promise<void>;
      maxBytes?: number;
      reserveBytes?: number;
      maxPendingPerTarget?: number;
      retryMs?: number;
      replayBatch?: number;
      replayConcurrency?: number;
      maxInFlight?: number;
      maxInFlightBytes?: number;
      destinationId?: string;
      canReplay?: () => boolean;
      onFault?: (message: string) => void;
    },
  ) {
    this.maxBytes = options.maxBytes ?? 1024 * 1024 * 1024;
    this.reserveBytes = options.reserveBytes ?? 64 * 1024 * 1024;
    this.maxPendingPerTarget = options.maxPendingPerTarget ?? 1000000;
    if (!Number.isSafeInteger(this.maxBytes) || this.maxBytes < 1024 * 1024)
      throw new Error("Invalid journal budget");
    mkdirSync(options.directory, { recursive: true, mode: 0o700 });
    const directory = lstatSync(options.directory);
    if (
      !directory.isDirectory() ||
      directory.isSymbolicLink() ||
      directory.uid !== process.getuid?.() ||
      directory.mode & 0o077
    )
      throw new Error("Target journal requires a private, owned directory");
    const file = (name: string) => {
      const value = path.join(options.directory, name);
      if (existsSync(value)) {
        const stat = lstatSync(value);
        if (
          !stat.isFile() ||
          stat.isSymbolicLink() ||
          stat.nlink !== 1 ||
          stat.uid !== directory.uid ||
          stat.mode & 0o077
        )
          throw new Error("Unsafe target journal file");
      } else closeSync(openSync(value, "wx", 0o600));
      return value;
    };
    this.lease = new DatabaseSync(file("owner.sqlite"));
    try {
      this.lease.exec("PRAGMA locking_mode=EXCLUSIVE; BEGIN EXCLUSIVE;");
    } catch (error) {
      this.lease.close();
      throw error;
    }
    try {
      for (const name of ["queue.sqlite-wal", "queue.sqlite-shm"]) {
        if (existsSync(path.join(options.directory, name))) file(name);
      }
      this.db = new DatabaseSync(file("queue.sqlite"));
      this.db
        .exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=64;
        PRAGMA max_page_count=${Math.floor(this.maxBytes / 4096)};`);
      const version = Number(
        this.db.prepare("PRAGMA user_version").get()!.user_version,
      );
      if (version !== 0 && version !== 1)
        throw new Error("Unsupported target journal version");
      this.db
        .exec(`CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS queue (seq INTEGER PRIMARY KEY, id TEXT UNIQUE NOT NULL,
          target TEXT NOT NULL, topic TEXT NOT NULL, payload TEXT NOT NULL, legacy_key TEXT,
          state TEXT NOT NULL DEFAULT 'pending', bytes INTEGER NOT NULL, created INTEGER NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0, retry_at INTEGER NOT NULL DEFAULT 0,
          in_write INTEGER NOT NULL DEFAULT 0);
        CREATE INDEX IF NOT EXISTS queue_target ON queue(target, state, seq);
        CREATE INDEX IF NOT EXISTS queue_target_oldest ON queue(target, state, created);
        CREATE INDEX IF NOT EXISTS queue_legacy ON queue(legacy_key);
        CREATE INDEX IF NOT EXISTS queue_confirmed_live ON queue(seq DESC)
          WHERE state='confirmed' AND legacy_key IS NULL;
        CREATE TABLE IF NOT EXISTS totals (target TEXT PRIMARY KEY, pending INTEGER NOT NULL DEFAULT 0,
          bytes INTEGER NOT NULL DEFAULT 0, failures INTEGER NOT NULL DEFAULT 0, last_failure INTEGER,
          retrying INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS observations (topic TEXT NOT NULL, target TEXT NOT NULL, PRIMARY KEY(topic,target));
        CREATE TABLE IF NOT EXISTS commands (id TEXT PRIMARY KEY, actor TEXT NOT NULL, hash TEXT NOT NULL,
          response TEXT NOT NULL, created INTEGER NOT NULL);
        PRAGMA user_version=1;`);
      // A process crash can lose the ACK after QuestDB committed. Never report
      // those admitted writes as a known quiescent boundary on restart.
      this.transaction(() => {
        this.db
          .exec(`UPDATE totals SET retrying=retrying+(SELECT count(*) FROM queue q
          WHERE q.target=totals.target AND q.state='pending' AND q.in_write=1 AND q.attempts=0);
          UPDATE queue SET attempts=attempts+1,in_write=0 WHERE state='pending' AND in_write=1;`);
      });
      const destination = this.db
        .prepare("SELECT value FROM meta WHERE key='destination'")
        .get();
      if (
        destination &&
        destination.value !== (options.destinationId ?? "unspecified")
      )
        throw new Error(
          "Retained journal destination changed; review required before replay",
        );
      this.db
        .prepare("INSERT OR IGNORE INTO meta VALUES ('destination',?)")
        .run(options.destinationId ?? "unspecified");
      const stored = this.db
        .prepare("SELECT value FROM meta WHERE key='policy'")
        .get();
      const portable = existsSync(options.policyFile)
        ? this.readPortable()
        : undefined;
      if (stored) {
        this.policy = policySchema.parse(JSON.parse(String(stored.value)));
        if (
          portable &&
          (portable.revision > this.policy.revision ||
            (portable.revision === this.policy.revision &&
              JSON.stringify(portable) !== JSON.stringify(this.policy)))
        )
          throw new Error(
            "Target pause policy conflicts with journal; review required",
          );
      } else {
        this.policy = portable ?? this.policy;
        this.db
          .prepare("INSERT INTO meta VALUES ('policy',?)")
          .run(JSON.stringify(this.policy));
      }
      this.mirrorPolicy();
      // fsync the directory entries for the newly created databases.
      this.syncDirectory(options.directory);
    } catch (error) {
      this.db?.close();
      this.lease.close();
      throw error;
    }
  }

  private syncDirectory(directory: string) {
    const descriptor = openSync(directory, "r");
    try {
      fsyncSync(descriptor);
    } finally {
      closeSync(descriptor);
    }
  }
  private readPortable(): Policy {
    const stat = lstatSync(this.options.policyFile);
    if (
      !stat.isFile() ||
      stat.isSymbolicLink() ||
      stat.nlink !== 1 ||
      stat.size > 256 * 1024
    )
      throw new Error("Unsafe target pause policy");
    return policySchema.parse(
      JSON.parse(readFileSync(this.options.policyFile, "utf8")),
    );
  }
  private mirrorPolicy() {
    // SQLite is authoritative. An unpaused owner needs no separate file
    // transferred by the host; the SDK carries its bounded application state.
    if (!this.globalPaused && this.policy.rules.length === 0) {
      if (existsSync(this.options.policyFile)) {
        this.readPortable(); // Never unlink an unsafe/foreign path.
        unlinkSync(this.options.policyFile);
        this.syncDirectory(path.dirname(this.options.policyFile));
      }
      return;
    }
    const temporary = `${this.options.policyFile}.${process.pid}.tmp`;
    try {
      writeFileSync(temporary, JSON.stringify(this.policy), {
        flag: "wx",
        mode: 0o600,
      });
      const fd = openSync(temporary, "r");
      try {
        fsyncSync(fd);
      } finally {
        closeSync(fd);
      }
      renameSync(temporary, this.options.policyFile);
      this.syncDirectory(path.dirname(this.options.policyFile));
    } finally {
      if (existsSync(temporary)) unlinkSync(temporary);
    }
  }
  setGlobalPaused(paused: boolean) {
    if (this.closed)
      throw new Error("Closed target journal cannot change pause state");
    const previous = this.globalPaused;
    this.globalPaused = paused;
    try {
      this.mirrorPolicy();
    } catch (error) {
      this.globalPaused = previous;
      this.fault(error);
    }
  }
  private transaction<R>(work: () => R): R {
    const previousPolicy = structuredClone(this.policy);
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const result = work();
      this.db.exec("COMMIT");
      return result;
    } catch (error) {
      this.db.exec("ROLLBACK");
      this.policy = previousPolicy;
      throw error;
    }
  }
  private fault(error: unknown): never {
    // Avoid logging payloads, SQL or destination credentials.
    this.storageError =
      "Durable target journal failed; acceptance is not confirmed. Check disk capacity and permissions.";
    this.options.onFault?.(this.storageError);
    throw new TargetStorageError(error);
  }
  private savePolicy(policy: Policy) {
    if (Buffer.byteLength(JSON.stringify(policy)) > 192 * 1024)
      throw new Error("Portable target policy budget exceeded");
    this.db
      .prepare("INSERT OR REPLACE INTO meta VALUES ('policy',?)")
      .run(JSON.stringify(policy));
    this.policy = policy;
  }
  private retainedWritesBlockPolicy(sourcePreflight = false): boolean {
    // SDK checks the still-active source before freezing admission. Healthy
    // writes already in progress may finish in its drain hook. Never admit
    // retries, queued work or uncertain outcomes through this preliminary check.
    return (
      !!this.storageError ||
      (!sourcePreflight && this.jobs.size > 0) ||
      !!this.db
        .prepare(
          sourcePreflight
            ? "SELECT 1 FROM queue WHERE state='pending' AND (in_write=0 OR attempts>0) LIMIT 1"
            : "SELECT 1 FROM queue WHERE state='pending' LIMIT 1",
        )
        .get()
    );
  }
  exportPolicy(options: { sourcePreflight?: boolean } = {}) {
    if (this.globalPaused)
      throw new Error(
        "Archiver migration is blocked while archiving is paused. Resume archiving before migration.",
      );
    if (this.policy.rules.length)
      throw new Error(
        "Archiver migration is blocked by a target pause. Resume all target pauses before migration.",
      );
    if (this.closed) {
      if (!this.closedDrained || this.storageError)
        throw new Error("Closed target journal retained pending writes");
      return structuredClone(this.policy);
    }
    if (this.retainedWritesBlockPolicy(options.sourcePreflight))
      throw new Error(
        "Target journal is not drained; retain this source and its queue before handover",
      );
    return structuredClone(this.policy);
  }
  validatePolicy(value: unknown, options: { sourcePreflight?: boolean } = {}) {
    const policy = policySchema.parse(value);
    if (this.globalPaused || this.policy.rules.length || policy.rules.length)
      throw new Error(
        "Archiver migration is blocked by a target pause. Resume all target pauses before migration.",
      );
    if (this.closed) {
      if (!this.closedDrained || this.storageError)
        throw new Error("Closed target journal retained pending writes");
      if (JSON.stringify(policy) !== JSON.stringify(this.policy))
        throw new Error(
          "Closed target journal can validate only its own final policy",
        );
      return;
    }
    if (
      JSON.stringify(policy) !== JSON.stringify(this.policy) &&
      this.policy.revision !== 0
    )
      throw new Error("Retained target pause policy differs; review required");
    if (this.retainedWritesBlockPolicy(options.sourcePreflight))
      throw new Error("Target has retained writes; policy replacement blocked");
  }
  importPolicy(value: unknown) {
    if (this.closed)
      throw new Error("Closed target journal cannot import handover state");
    this.validatePolicy(value);
    try {
      this.savePolicy(policySchema.parse(value));
      this.mirrorPolicy();
    } catch (error) {
      this.fault(error);
    }
  }
  private observe(topic: string, target: string) {
    if (
      this.db
        .prepare("SELECT 1 FROM observations WHERE topic=? AND target=?")
        .get(topic, target)
    )
      return;
    if (
      Number(
        this.db.prepare("SELECT count(*) AS n FROM observations").get()!.n,
      ) >= 100000
    )
      throw new Error("Target observation budget exhausted; review required");
    this.db.prepare("INSERT INTO observations VALUES (?,?)").run(topic, target);
    const rules = this.policy.rules.map((rule) =>
      rule.selector.topicFilter &&
      TopicMatcher.matches(rule.selector.topicFilter, topic) &&
      !rule.tables.includes(target)
        ? { ...rule, tables: [...rule.tables, target].sort() }
        : rule,
    );
    if (JSON.stringify(rules) !== JSON.stringify(this.policy.rules))
      this.savePolicy(policySchema.parse({ ...this.policy, rules }));
  }
  isPaused(target: string) {
    return this.policy.rules.some((rule) => rule.tables.includes(target));
  }
  preview(selector: Selector) {
    const parsed = selectorSchema.parse(selector);
    const targets =
      parsed.tables ??
      [
        ...new Set(
          (
            this.db.prepare("SELECT topic,target FROM observations").all() as {
              topic: string;
              target: string;
            }[]
          )
            .filter((row) =>
              TopicMatcher.matches(parsed.topicFilter!, row.topic),
            )
            .map((row) => row.target),
        ),
      ].sort();
    return {
      selector: parsed,
      tables: targets,
      newDataGroups: parsed.topicFilter
        ? "pause-new-matching-final-tables"
        : "explicit-tables-only",
      scope:
        "All Archiver-owned writes to each final table, including other topics. External writers are not controlled.",
      revision: this.policy.revision,
    };
  }

  /** Atomic admission of all packet parts. Different equal MQTT occurrences need different IDs. */
  async submit(entries: JournalEntry<T>[]): Promise<boolean> {
    if (this.closed) throw new Error("Journal closed");
    try {
      const before = JSON.stringify(this.policy);
      this.transaction(() => {
        for (const entry of entries) {
          table.parse(entry.table);
          if (entry.id.length > 200 || !entry.id || entry.topic.length > 4096)
            throw new Error("Invalid target envelope");
          this.observe(entry.topic, entry.table);
          if (this.db.prepare("SELECT 1 FROM queue WHERE id=?").get(entry.id))
            continue;
          const payload = JSON.stringify(entry.payload);
          const bytes =
            Buffer.byteLength(payload) + Buffer.byteLength(entry.topic) + 512;
          const free = statfsSync(this.options.directory);
          const total = Number(
            this.db
              .prepare("SELECT coalesce(sum(bytes),0) AS n FROM totals")
              .get()!.n,
          );
          const pending = Number(
            this.db
              .prepare("SELECT pending FROM totals WHERE target=?")
              .get(entry.table)?.pending ?? 0,
          );
          if (
            bytes > 16 * 1024 * 1024 ||
            total + bytes > this.maxBytes / 2 ||
            pending >= this.maxPendingPerTarget ||
            Number(free.bavail) * Number(free.bsize) < this.reserveBytes + bytes
          )
            throw new Error("Durable target journal capacity exhausted");
          this.db
            .prepare(
              "INSERT INTO queue(id,target,topic,payload,legacy_key,bytes,created) VALUES (?,?,?,?,?,?,?)",
            )
            .run(
              entry.id,
              entry.table,
              entry.topic,
              payload,
              entry.legacyKey ?? null,
              bytes,
              Date.now(),
            );
          this.db
            .prepare(
              `INSERT INTO totals(target,pending,bytes) VALUES (?,1,?) ON CONFLICT(target)
            DO UPDATE SET pending=pending+1,bytes=bytes+excluded.bytes`,
            )
            .run(entry.table, bytes);
        }
      });
      if (JSON.stringify(this.policy) !== before) this.mirrorPolicy();
      this.storageError = null;
    } catch (error) {
      this.fault(error);
    }
    await Promise.all(
      [...new Set(entries.map((entry) => entry.table))].map((target) =>
        this.tryHead(target),
      ),
    );
    await Promise.all(
      entries.flatMap((entry) => this.jobs.get(entry.id) ?? []),
    );
    return entries.every(
      (entry) =>
        this.db.prepare("SELECT state FROM queue WHERE id=?").get(entry.id)
          ?.state === "confirmed",
    );
  }

  /** Receipts survive until the original live/stored/legacy envelope is acknowledged. */
  acknowledge(ids: string[]) {
    try {
      for (const id of ids)
        this.db
          .prepare(
            "DELETE FROM queue WHERE id=? AND state='confirmed' AND legacy_key IS NULL",
          )
          .run(id);
    } catch (error) {
      this.fault(error);
    }
  }
  acknowledgeLegacy(key: string) {
    try {
      this.db
        .prepare("DELETE FROM queue WHERE legacy_key=? AND state='confirmed'")
        .run(key);
    } catch (error) {
      this.fault(error);
    }
  }
  private async tryHead(target: string, batch = 1): Promise<void> {
    if (this.closed || this.isPaused(target) || this.storageError || this.options.canReplay?.() === false) return;
    const rows = this.db
      .prepare(
        "SELECT seq,id,retry_at,bytes FROM queue WHERE target=? AND state='pending' ORDER BY seq LIMIT 1024",
      )
      .all(target);
    const selected: Promise<void>[] = [];
    for (const row of rows) {
      if (this.jobs.has(String(row.id))) continue;
      if (
        Number(row.retry_at) > Date.now() ||
        selected.length >= batch ||
        this.jobs.size >= (this.options.maxInFlight ?? 512) ||
        this.activeBytes + Number(row.bytes) >
          (this.options.maxInFlightBytes ?? 16 * 1024 * 1024)
      )
        break;
      selected.push(
        this.startWrite(
          this.db
            .prepare("SELECT * FROM queue WHERE seq=?")
            .get(row.seq) as Row,
        ),
      );
    }
    await Promise.all(selected);
  }
  private startWrite(row: Row): Promise<void> {
    const target = row.target;
    this.activeBytes += row.bytes;
    // Install the promise synchronously before any write can yield. Pause captures this exact set.
    const running = Promise.resolve()
      .then(async () => {
        try {
          this.db
            .prepare("UPDATE queue SET in_write=1 WHERE seq=?")
            .run(row.seq);
        } catch (error) {
          this.fault(error);
        }
        try {
          await this.options.write({
            id: row.id,
            table: target,
            topic: row.topic,
            payload: JSON.parse(row.payload),
            legacyKey: row.legacy_key ?? undefined,
          });
        } catch {
          try {
            const delay = Math.min(
              30000,
              (this.options.retryMs ?? 1000) * 2 ** Math.min(row.attempts, 5),
            );
            this.transaction(() => {
              this.db
                .prepare(
                  "UPDATE queue SET attempts=attempts+1,retry_at=?,in_write=0 WHERE seq=?",
                )
                .run(Date.now() + delay, row.seq);
              this.db
                .prepare(
                  "UPDATE totals SET failures=failures+1,last_failure=?,retrying=retrying+? WHERE target=?",
                )
                .run(Date.now(), row.attempts === 0 ? 1 : 0, target);
            });
          } catch (error) {
            this.fault(error);
          }
          return;
        }
        try {
          this.transaction(() => {
            this.db
              .prepare(
                "UPDATE totals SET pending=pending-1,bytes=bytes-(SELECT bytes FROM queue WHERE seq=?),retrying=retrying-? WHERE target=?",
              )
              .run(row.seq, row.attempts > 0 ? 1 : 0, target);
            this.db
              .prepare(
                "UPDATE queue SET state='confirmed',payload='',bytes=0,in_write=0 WHERE seq=?",
              )
              .run(row.seq);
          });
        } catch (error) {
          this.fault(error);
        }
      })
      .finally(() => {
        this.jobs.delete(row.id);
        this.active.get(target)?.delete(running);
        this.activeBytes -= row.bytes;
      });
    this.jobs.set(row.id, running);
    if (!this.active.has(target)) this.active.set(target, new Set());
    this.active.get(target)!.add(running);
    return running;
  }

  async command(raw: unknown, actor: string, timeoutMs = 10000) {
    const command = targetCommandSchema.parse(raw);
    if (command.action === "preview") return this.preview(command.selector);
    if (!actor || actor.length > 200)
      throw new TargetControlError(403, "Operator identity required");
    if (this.closed) throw new TargetControlError(503, "Journal closed");
    if (this.storageError) {
      // A full logical queue must still be resumable once durable control writes work.
      try {
        this.db
          .prepare("INSERT OR REPLACE INTO meta VALUES ('storage-probe',?)")
          .run(String(Date.now()));
        this.mirrorPolicy();
        this.storageError = null;
      } catch (error) {
        this.fault(error);
      }
    }
    const hash = createHash("sha256")
      .update(JSON.stringify(command))
      .digest("hex");
    const prior = this.db
      .prepare("SELECT * FROM commands WHERE id=?")
      .get(command.requestId);
    if (prior) {
      if (prior.actor !== actor || prior.hash !== hash)
        throw new TargetControlError(
          409,
          "requestId belongs to another command",
        );
      // Lost-response retry returns the same recorded operation, plus current boundary evidence.
      return {
        ...JSON.parse(String(prior.response)),
        boundary: this.boundary(),
      };
    }
    if (this.policyBusy)
      throw new TargetControlError(
        409,
        "Another target command is draining; read status",
      );
    if (command.expectedRevision !== this.policy.revision)
      throw new TargetControlError(409, "Stale target policy revision");
    const rules = structuredClone(this.policy.rules);
    if (command.action === "pause") {
      if (rules.length >= 64)
        throw new TargetControlError(409, "Pause rule limit reached");
      rules.push({
        id: command.requestId,
        selector: command.selector,
        tables: this.preview(command.selector).tables,
        actor,
        reason: command.reason,
        at: new Date().toISOString(),
      });
    } else {
      const index = rules.findIndex((rule) => rule.id === command.pauseId);
      if (index < 0) throw new TargetControlError(404, "Unknown pauseId");
      if (this.boundary().inFlight > 0)
        throw new TargetControlError(
          409,
          "Wait for in-flight writes before resuming",
        );
      if (
        this.boundary().phase === "write-outcome-uncertain" &&
        !command.confirmUnknownWriteOutcome
      )
        throw new TargetControlError(
          409,
          "A write outcome is uncertain; reconcile database visibility before explicit confirmation",
        );
      rules.splice(index, 1);
    }
    this.policyBusy = true;
    const response = {
      requestId: command.requestId,
      action: command.action,
      actor,
      reason: command.reason,
      at: new Date().toISOString(),
      revision: this.policy.revision + 1,
      pauseId: command.action === "pause" ? command.requestId : command.pauseId,
    };
    try {
      this.transaction(() => {
        this.savePolicy(
          policySchema.parse({
            protocol: 1,
            revision: response.revision,
            rules,
          }),
        );
        this.db
          .prepare("INSERT INTO commands VALUES (?,?,?,?,?)")
          .run(
            command.requestId,
            actor,
            hash,
            JSON.stringify(response),
            Date.now(),
          );
      });
      this.mirrorPolicy();
      if (command.action === "pause") {
        const jobs = [...this.active]
          .filter(([target]) => this.isPaused(target))
          .flatMap(([, pending]) => [...pending]);
        let timer: ReturnType<typeof setTimeout> | undefined;
        try {
          await Promise.race([
            Promise.allSettled(jobs),
            new Promise((resolve) => {
              timer = setTimeout(resolve, timeoutMs);
            }),
          ]);
        } finally {
          clearTimeout(timer);
        }
      }
      return { ...response, boundary: this.boundary() };
    } catch (error) {
      this.fault(error);
    } finally {
      this.policyBusy = false;
    }
  }
  private boundary() {
    if (!this.policy.rules.length)
      return {
        phase: "running",
        inFlight: 0,
        guarantee:
          "No selective pause is active. Database acknowledgement and visibility are reported separately.",
      };
    const inFlight = [...this.active]
      .filter(([target]) => this.isPaused(target))
      .reduce((n, [, values]) => n + values.size, 0);
    const failed = this.db
      .prepare(`SELECT target FROM totals WHERE retrying>0`)
      .all()
      .some((row) => this.isPaused(String(row.target)));
    return {
      phase: inFlight
        ? "draining"
        : failed
          ? "write-outcome-uncertain"
          : "paused",
      inFlight,
      guarantee:
        "No new Archiver submissions to paused tables. ILP flush acknowledgement is not WAL visibility or control of other writers.",
    };
  }
  snapshot() {
    const targets = this.db
      .prepare(
        `SELECT t.*, (SELECT min(created) FROM queue q WHERE q.target=t.target AND state='pending') AS oldest
      FROM totals t ORDER BY target LIMIT 1001`,
      )
      .all();
    const pages = Number(
      this.db.prepare("PRAGMA page_count").get()!.page_count,
    );
    const free = statfsSync(this.options.directory);
    return {
      protocol: 1,
      policy: structuredClone(this.policy),
      boundary: this.boundary(),
      healthy: !this.storageError,
      durability: this.storageError ? "failed" : "sqlite-full",
      storageError: this.storageError,
      diskBudgetBytes: this.maxBytes,
      payloadBudgetBytes: Math.floor(this.maxBytes / 2),
      maxPendingPerTarget: this.maxPendingPerTarget,
      reserveBytes: this.reserveBytes,
      inFlightBytes: this.activeBytes,
      diskFreeBytes: Number(free.bavail) * Number(free.bsize),
      databaseBytes: pages * 4096,
      walBytes: existsSync(
        path.join(this.options.directory, "queue.sqlite-wal"),
      )
        ? lstatSync(path.join(this.options.directory, "queue.sqlite-wal")).size
        : 0,
      targetLimitReached: targets.length > 1000,
      targets: targets.slice(0, 1000).map((row) => ({
        table: row.target,
        paused: this.isPaused(String(row.target)),
        queued: row.pending,
        bytes: row.bytes,
        failures: row.failures,
        retrying: row.retrying,
        lastFailureAt: row.last_failure,
        oldestAgeMs:
          row.oldest === null ? null : Date.now() - Number(row.oldest),
        inFlight: this.active.get(String(row.target))?.size ?? 0,
      })),
    };
  }
  replay(): Promise<void> {
    if (this.replaying) return this.replaying;
    this.replaying = this.replayPass().finally(() => {
      this.replaying = undefined;
    });
    return this.replaying;
  }
  private async replayPass() {
    if (this.closed || this.options.canReplay?.() === false) return;
    const batch = this.options.replayBatch ?? 64;
    const rows = this.db
      .prepare("SELECT target FROM totals WHERE pending>0 ORDER BY target")
      .all();
    const targets = rows
      .map((row) => String(row.target))
      .filter((target) => !this.isPaused(target));
    const split = targets.findIndex((target) => target > this.cursor);
    const fair =
      split < 0
        ? targets
        : [...targets.slice(split), ...targets.slice(0, split)];
    if (!fair.length) return;
    const count = Math.min(batch, fair.length);
    const concurrency = Math.min(this.options.replayConcurrency ?? 8, count);
    let index = 0;
    await Promise.all(
      Array.from({ length: concurrency }, async () => {
        while (index < count) {
          const target = fair[index++];
          this.cursor = target;
          await this.tryHead(target, Math.max(1, Math.floor(batch / count)));
        }
      }),
    );
    // Keep live receipts bounded without expiring legacy acknowledgements.
    // The receipt-only index avoids reading every retained payload after a
    // long pause. Select primary keys first: the legacy-key index otherwise
    // makes SQLite scan the pending backlog to test its confirmation state.
    this.db
      .prepare(
        `DELETE FROM queue WHERE seq IN
      (SELECT seq FROM queue INDEXED BY queue_confirmed_live
       WHERE state='confirmed' AND legacy_key IS NULL ORDER BY seq DESC LIMIT -1 OFFSET 10000)`,
      )
      .run();
    this.db.exec("PRAGMA wal_checkpoint(PASSIVE)");
  }
  start() {
    this.timer ??= setInterval(() => {
      void this.replay().catch((error) => {
        try {
          this.fault(error);
        } catch {}
      });
    }, 250);
    this.timer.unref();
  }
  close(): Promise<void> {
    return (this.closing ??= this.closeJournal());
  }
  private async closeJournal() {
    this.closed = true;
    clearInterval(this.timer);
    await this.replaying;
    await Promise.allSettled([...this.jobs.values()]);
    this.closedDrained = !this.db
      .prepare("SELECT 1 FROM queue WHERE state='pending' LIMIT 1")
      .get();
    this.db.close();
    this.lease.close();
  }
}
