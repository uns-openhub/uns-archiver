import { Sender } from "@questdb/nodejs-client";
import { IUnsPacket } from "@uns-kit/core";
import { NonRetryableError } from "../errors.js";
import { UnsTopicMetadata } from "../active-uns-topics.js";
import { buildQuestDbTableName } from "../questdb-table-name.js";
import { createHash } from "crypto";
import { logger } from "@uns-kit/core";
import { CircuitBreaker, errorMessage, isRetryableNetworkError, withRetry } from "../resilience.js";
import { deriveQuestDbTopicIdentity, normalizeQuestDbFullTopic } from "./questdb-identity.js";
import {
  canonicalTableColumnEntries,
  type CanonicalTableColumnEntry,
} from "./table-columns.js";
import {
  QuestDbIlpBatcher,
  type QuestDbBatchConfig,
  type QuestDbBatchDiagnostics,
} from "./questdb-ilp-batcher.js";


type QuestDbExecConfig = {
  url: string;
  headers: Record<string, string>;
};

const parseQuestDbConfigurationString = (configurationString: string | undefined): Map<string, string> => {
  const values = new Map<string, string>();
  const normalized = configurationString?.replace(/^[a-z]+::/i, "") ?? "";
  for (const part of normalized.split(";")) {
    const separatorIndex = part.indexOf("=");
    if (separatorIndex <= 0) continue;
    const key = part.slice(0, separatorIndex).trim();
    const value = part.slice(separatorIndex + 1).trim();
    if (key) values.set(key, value);
  }
  return values;
};

const parseHttpExecConfig = (configurationString: string | undefined): QuestDbExecConfig | null => {
  if (!configurationString) return null;
  const values = parseQuestDbConfigurationString(configurationString);
  const addr = values.get("addr");
  if (!addr) return null;
  const protocol = configurationString.startsWith("https::") ? "https" : "http";
  const username = values.get("username");
  const password = values.get("password");
  const headers: Record<string, string> = { Accept: "application/json" };
  if (username && password) {
    headers.Authorization = `Basic ${Buffer.from(`${username}:${password}`).toString("base64")}`;
  }
  return { url: `${protocol}://${addr}/exec`, headers };
};

type IngestMode = "append" | "dedup" | "window_replace" | undefined;

export type QuestDbDependencyHealth = {
  id: "questdb";
  label: "QuestDB";
  state: "healthy" | "degraded" | "unknown";
  healthy: boolean | null;
  checkedAt: string;
  message?: string;
};

export class QuestDBWriter {
  private readonly questDbExecConfig: QuestDbExecConfig | null;
  private readonly clearedWindows = new Set<string>();
  private readonly createdTables = new Set<string>();
  private readonly creatingTables = new Map<string, Promise<void>>();
  private readonly pendingWrites = new Map<string, Promise<void>>();
  private readonly execCircuit = new CircuitBreaker("questdb-exec", {
    failureThreshold: 5,
    openMs: 15000,
  });
  private readonly ilpCircuit = new CircuitBreaker("questdb-ilp-write", {
    failureThreshold: 5,
    openMs: 15000,
  });
  private readonly traceIngest =
    process.env.UNS_ARCHIVER_TRACE === "1" || process.env.UNS_ARCHIVER_TRACE_INGEST === "1";
  private readonly batcher: QuestDbIlpBatcher;

  constructor(
    private readonly questDbOutput: Sender,
    configurationString?: string,
    batchConfig?: QuestDbBatchConfig,
  ) {
    this.questDbExecConfig = parseHttpExecConfig(configurationString);
    this.batcher = new QuestDbIlpBatcher(
      {
        flush: async () =>
          await this.ilpCircuit.execute(async () => await this.questDbOutput.flush()),
        reset: () => this.resetSenderBuffer(),
        shouldRetry: (error) => this.isRetryableIlpError(error),
        onRetry: ({ attempt, delayMs, error, rows }) => {
          logger.warn(
            `QuestDB ILP batch retry for ${rows} rows (attempt ${attempt}) in ${delayMs}ms: ${errorMessage(error)}`,
          );
        },
        onFlush: ({ rows, durationMs }) => {
          logger.debug(`QuestDB ILP batch flushed rows=${rows} durationMs=${durationMs}.`);
        },
        onFailure: ({ rows, durationMs, error }) => {
          logger.error(
            `QuestDB ILP batch failed rows=${rows} durationMs=${durationMs}: ${errorMessage(error)}`,
          );
        },
      },
      batchConfig,
    );
  }

  async checkHealth(): Promise<QuestDbDependencyHealth> {
    const checkedAt = new Date().toISOString();
    if (!this.questDbExecConfig) {
      return {
        id: "questdb",
        label: "QuestDB",
        state: "unknown",
        healthy: null,
        checkedAt,
        message: "QuestDB HTTP exec URL is not configured.",
      };
    }

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(new Error("QuestDB health check timed out")), 2_000);
    try {
      const url = this.buildExecQueryUrl("SELECT 1");
      const res = await fetch(url, { headers: this.questDbExecConfig.headers, signal: controller.signal });
      if (!res.ok) {
        return {
          id: "questdb",
          label: "QuestDB",
          state: "degraded",
          healthy: false,
          checkedAt,
          message: `QuestDB exec returned HTTP ${res.status}.`,
        };
      }
      return {
        id: "questdb",
        label: "QuestDB",
        state: "healthy",
        healthy: true,
        checkedAt,
      };
    } catch (err) {
      return {
        id: "questdb",
        label: "QuestDB",
        state: "degraded",
        healthy: false,
        checkedAt,
        message: err instanceof Error ? err.message : String(err),
      };
    } finally {
      clearTimeout(timer);
    }
  }

  // Add a public method to close the questDbOutput connection
  async close() {
    await this.batcher.close();
    await this.questDbOutput.close();
  }

  configureBatch(config?: QuestDbBatchConfig): void {
    this.batcher.configure(config);
  }

  getBatchDiagnostics(): QuestDbBatchDiagnostics {
    return this.batcher.snapshot();
  }

  // Method to handle UnsPacketData
  async writeUnsPacket(
    unsPacket: IUnsPacket,
    matchingTable: string,
    inputTopic: any,
    topicMeta?: UnsTopicMetadata,
    ingestMode?: IngestMode,
  ) {
    const parseToMs = (field: string, value: unknown): number | null => {
      if (value === null || value === undefined) return null;
      if (typeof value === "number") {
        if (!Number.isFinite(value)) {
          throw new NonRetryableError(`Field '${field}' expected finite number for epoch ms`);
        }
        return Math.trunc(value);
      }
      const asDate = new Date(String(value));
      const ms = asDate.getTime();
      if (!Number.isFinite(ms)) {
        throw new NonRetryableError(`Field '${field}' expected ISO8601 or epoch ms`);
      }
      return ms;
    };

    const deriveEventId = (
      explicitId: string | undefined,
      fullTopic: string,
      intervalStart?: number | null,
      intervalEnd?: number | null,
    ) => {
      if (explicitId) return explicitId;
      if (intervalStart !== null || intervalEnd !== null) {
        const payload = [fullTopic, intervalStart ?? "null", intervalEnd ?? "null"].join("|");
        return createHash("sha256").update(payload).digest("hex");
      }
      return undefined;
    };

    const fullTopic = normalizeQuestDbFullTopic(inputTopic);
    const topicIdentity = deriveQuestDbTopicIdentity(fullTopic, topicMeta);
    const topicTag = topicIdentity.topic;
    const addCommonSymbols = (builder: any) => {
      let next = builder;
      const attribute = topicIdentity.attribute;
      const asset = topicIdentity.asset;
      const objectType = topicIdentity.objectType;
      const objectId = topicIdentity.objectId;
      if (attribute) next = next.symbol("attribute", attribute);
      if (asset) next = next.symbol("asset", asset);
      if (objectType) next = next.symbol("objectType", objectType);
      if (objectId) next = next.symbol("objectId", objectId);
      return next;
    };

    if (unsPacket.message.data) {
      const interval = unsPacket.interval ?? null;
      const createdAt = unsPacket.message.createdAt ? new Date(unsPacket.message.createdAt).getTime() : null;
      const expiresAt = unsPacket.message.expiresAt ? new Date(unsPacket.message.expiresAt).getTime() : null;
      const time = unsPacket.message.data.time ? new Date(unsPacket.message.data.time).getTime() : 0;
      const dataValue = unsPacket.message.data.value;
      const value = typeof dataValue === "number" ? dataValue : null;
      const stringValue = typeof dataValue === "string" ? dataValue : null;
      const numberValue = typeof dataValue === "number" ? dataValue : null;
      const uom = unsPacket.message.data.uom ?? null;
      const foreignEventKey = unsPacket.message.data.foreignEventKey ?? null;
      const valueType = unsPacket.message.data.valueType ?? null;
      const intervalStart = parseToMs("intervalStart", (unsPacket.message.data as any).intervalStart);
      const intervalEnd = parseToMs("intervalEnd", (unsPacket.message.data as any).intervalEnd);
      const windowStart = parseToMs("windowStart", (unsPacket.message.data as any).windowStart ?? (unsPacket.message as any).windowStart);
      const windowEnd = parseToMs("windowEnd", (unsPacket.message.data as any).windowEnd ?? (unsPacket.message as any).windowEnd);
      const explicitEventId = (unsPacket.message.data as any).eventId as string | undefined;
      const deleted = (unsPacket.message.data as any).deleted ?? false;
      const deletedAtRaw = parseToMs("deletedAt", (unsPacket.message.data as any).deletedAt);
      const deletedAt = deletedAtRaw !== null ? deletedAtRaw : deleted ? time : null;
      const lastSeenRaw = parseToMs("lastSeen", (unsPacket.message.data as any).lastSeen);
      const lastSeen = lastSeenRaw !== null ? lastSeenRaw : ingestMode === "window_replace" && windowEnd != null ? windowEnd : time;
      const eventId = deriveEventId(explicitEventId, fullTopic, intervalStart, intervalEnd);

      const dataGroup = unsPacket.message.data.dataGroup;
      const tableName = buildQuestDbTableName(matchingTable, dataGroup, "_data");

      await this.runWithTable(tableName, "data", ingestMode, [], async () => {
        let builder = this.questDbOutput.table(tableName).symbol("topic", topicTag);
        builder = addCommonSymbols(builder);
        if (eventId) {
          builder = builder.symbol("eventId", eventId);
        }
        if (valueType != null) {
          builder = builder.symbol("valueType", valueType);
        } else {
          builder = builder.symbol("valueType", typeof dataValue);
        }
        builder = builder.booleanColumn("deleted", deleted);
        builder = builder.timestampColumn("lastSeen", lastSeen, "ms");
        if (deletedAt != null) {
          builder = builder.timestampColumn("deletedAt", deletedAt, "ms");
        }

        if (value != null) {
          builder = builder.floatColumn("value", value);
        }
        if (stringValue != null) {
          builder = builder.stringColumn("stringValue", stringValue);
        }
        if (numberValue != null) {
          builder = builder.floatColumn("numberValue", numberValue);
        }
        if (uom != null) {
          builder = builder.stringColumn("uom", uom);
        }
        if (foreignEventKey != null) {
          builder = builder.stringColumn("foreignEventKey", foreignEventKey);
        }
        if (createdAt != null) {
          builder = builder.timestampColumn("createdAt", createdAt, "ms");
        }
        if (expiresAt != null) {
          builder = builder.timestampColumn("expiresAt", expiresAt, "ms");
        }
        if (interval != null) {
          builder = builder.intColumn("interval", interval);
        }
        if (intervalStart != null) {
          builder = builder.timestampColumn("intervalStart", intervalStart, "ms");
        }
        if (intervalEnd != null) {
          builder = builder.timestampColumn("intervalEnd", intervalEnd, "ms");
        }

        await this.performWindowDeleteOnce(tableName, topicTag, windowStart, windowEnd, time, ingestMode);
        await builder.at(time, "ms");
      });
    }

    // Only data and table are supported in the new UNS message format.

    // If message has table
    if (unsPacket.message.table) {
      const time = unsPacket.message.table.time ? new Date(unsPacket.message.table.time).getTime() : 0;
      const createdAt = unsPacket.message.createdAt ? new Date(unsPacket.message.createdAt).getTime() : null;
      const expiresAt = unsPacket.message.expiresAt ? new Date(unsPacket.message.expiresAt).getTime() : null;
      const intervalStart = parseToMs("intervalStart", (unsPacket.message.table as any).intervalStart);
      const intervalEnd = parseToMs("intervalEnd", (unsPacket.message.table as any).intervalEnd);
      const windowStart = parseToMs("windowStart", (unsPacket.message.table as any).windowStart ?? (unsPacket.message as any).windowStart);
      const windowEnd = parseToMs("windowEnd", (unsPacket.message.table as any).windowEnd ?? (unsPacket.message as any).windowEnd);
      const explicitEventId = (unsPacket.message.table as any).eventId as string | undefined;
      const deleted = (unsPacket.message.table as any).deleted ?? false;
      const deletedAtRaw = parseToMs("deletedAt", (unsPacket.message.table as any).deletedAt);
      const deletedAt = deletedAtRaw !== null ? deletedAtRaw : deleted ? time : null;
      const lastSeenRaw = parseToMs("lastSeen", (unsPacket.message.table as any).lastSeen);
      const lastSeen = lastSeenRaw !== null ? lastSeenRaw : ingestMode === "window_replace" && windowEnd != null ? windowEnd : time;
      const eventId = deriveEventId(explicitEventId, fullTopic, intervalStart, intervalEnd);

      const dataGroup = unsPacket.message.table.dataGroup;
      const tableName = buildQuestDbTableName(matchingTable, dataGroup, "_table");
      const columns = canonicalTableColumnEntries(
        unsPacket.message.table.columns,
        tableName,
      );

      if (this.traceIngest) {
        const iso = (ms: number | null) => (ms == null ? "null" : new Date(ms).toISOString());
        logger.info(
          `[trace][questdb] table incoming tableName=${tableName} topicTag='${topicTag}' fullTopic='${fullTopic}' eventId=${eventId ?? "null"} time=${new Date(time).toISOString()} interval=[${iso(intervalStart)},${iso(intervalEnd)}] window=[${iso(windowStart)},${iso(windowEnd)}] deleted=${deleted} cols=${columns.length}`,
        );
      }

      await this.runWithTable(tableName, "table", ingestMode, columns, async () => {
        let builder = this.questDbOutput.table(tableName).symbol("topic", topicTag);
        builder = addCommonSymbols(builder);
        if (eventId) {
          builder = builder.symbol("eventId", eventId);
        }

        // SYMBOL columns must be written as tags (before any fields).
        for (const [name, column] of columns) {
          if (column?.type !== "symbol") continue;
          if (column.value === null || column.value === undefined) continue;
          builder = builder.symbol(name, String(column.value));
        }

        builder = builder.booleanColumn("deleted", deleted);
        builder = builder.timestampColumn("lastSeen", lastSeen, "ms");
        if (deletedAt != null) {
          builder = builder.timestampColumn("deletedAt", deletedAt, "ms");
        }

        for (const [name, column] of columns) {
          if (column.type === "symbol") continue;
          if (column.value === null || column.value === undefined) continue;

          const value = column.value;
          const colType = column.type as string;
          switch (colType) {
            case "varchar":
            case "string":
            case "char":
            case "uuid":
            case "ipv4":
            case "binary":
            case "long256":
              builder = builder.stringColumn(name, String(value));
              break;
            case "float":
            case "double": {
              const n = typeof value === "number" ? value : Number(value);
              if (Number.isNaN(n)) {
                throw new NonRetryableError(`Column '${name}' expected number for type ${colType}`);
              }
              builder = builder.floatColumn(name, n);
              break;
            }
            case "byte":
            case "short":
            case "int":
            case "long": {
              const n = typeof value === "number" ? value : Number(value);
              if (!Number.isFinite(n)) {
                throw new NonRetryableError(`Column '${name}' expected integer for type ${colType}`);
              }
              builder = builder.intColumn(name, Math.trunc(n));
              break;
            }
            case "boolean": {
              const b =
                typeof value === "boolean" ? value :
                  typeof value === "number" ? value !== 0 :
                    String(value).toLowerCase() === "true";
              builder = builder.booleanColumn(name, b);
              break;
            }
            case "date":
            case "timestamp": {
              const n = parseToMs(name, value);
              if (n === null) {
                throw new NonRetryableError(`Column '${name}' expected ISO8601 or epoch ms for type ${colType}`);
              }
              builder = builder.timestampColumn(name, n, "ms");
              break;
            }
            case "timestamp_ns": {
              if (typeof value === "number") {
                builder = builder.timestampColumn(name, BigInt(Math.trunc(value)), "ns");
                break;
              }
              const asString = String(value);
              if (!/^-?\\d+$/.test(asString)) {
                throw new NonRetryableError(`Column '${name}' expected integer ns for type timestamp_ns`);
              }
              builder = builder.timestampColumn(name, BigInt(asString), "ns");
              break;
            }
            default: {
              if (/^decimal\\(\\d+,\\d+\\)$/i.test(colType)) {
                builder = builder.stringColumn(name, String(value));
                break;
              }
              if (/^geohash\\(\\d+[bc]\\)$/i.test(colType)) {
                builder = builder.stringColumn(name, String(value));
                break;
              }
              if (colType.startsWith("array<")) {
                if (!Array.isArray(value)) {
                  throw new NonRetryableError(`Column '${name}' expected array for type ${colType}`);
                }
                builder = builder.arrayColumn(name, value as number[]);
                break;
              }
              throw new NonRetryableError(`Unsupported QuestDB column type '${colType}' for column '${name}'`);
            }
          }

          if (column.uom) {
            builder = builder.stringColumn(`${name}_uom`, column.uom);
          }
        }

        if (createdAt != null) {
          builder = builder.timestampColumn("createdAt", createdAt, "ms");
        }
        if (expiresAt != null) {
          builder = builder.timestampColumn("expiresAt", expiresAt, "ms");
        }
        if (intervalStart != null) {
          builder = builder.timestampColumn("intervalStart", intervalStart, "ms");
        }
        if (intervalEnd != null) {
          builder = builder.timestampColumn("intervalEnd", intervalEnd, "ms");
        }

        if (this.traceIngest) {
          logger.info(
            `[trace][questdb] table write tableName=${tableName} topicTag='${topicTag}' at=${new Date(time).toISOString()}`,
          );
        }
        await this.performWindowDeleteOnce(tableName, topicTag, windowStart, windowEnd, time, ingestMode);
        await builder.at(time, "ms");
      });
    }
  }

  /**
   * For windowed replays: delete existing rows in the provided window once per table/topic/window combo.
   *
   * IMPORTANT: QuestDB does not support row-level DELETE, so we implement "window_replace"
   * by soft-deleting any rows in the window that were not refreshed in the current window snapshot.
   *
   * Mechanism:
   * - For window_replace writes, we set `lastSeen = windowEnd` for each row we ingest.
   * - Once per (table/topic/window), we run:
   *     UPDATE ... SET deleted=true, deletedAt=windowEnd WHERE ... AND lastSeen < windowEnd
   *
   * This marks "missing" buckets as deleted, while buckets that arrive later in the same snapshot
   * are re-activated by their own writes (deleted=false, lastSeen=windowEnd).
   */
  private async performWindowDeleteOnce(
    tableName: string,
    topicTag: string,
    windowStart: number | null,
    windowEnd: number | null,
    fallbackTime: number,
    ingestMode: IngestMode,
  ): Promise<void> {
    if (!this.questDbExecConfig) return;
    if (ingestMode !== "window_replace") return;
    const execHeaders = this.questDbExecConfig.headers;
    if (windowStart === null || windowEnd === null) return;
    const startIso = new Date(windowStart).toISOString();
    const endIso = new Date(windowEnd).toISOString();
    const key = `${tableName}|${topicTag}|${startIso}|${endIso}`;
    if (this.clearedWindows.has(key)) {
      if (this.traceIngest) {
        logger.info(`[trace][questdb] window_delete skip key=${key}`);
      }
      return;
    }

    // Escape single quotes in topic
    const topicEscaped = topicTag.replace(/'/g, "''");
    const markIso = new Date(windowEnd ?? fallbackTime).toISOString();
    const query = [
      `UPDATE ${tableName}`,
      `SET deleted=true, deletedAt='${markIso}'`,
      `WHERE topic='${topicEscaped}'`,
      `AND time >= '${startIso}'`,
      `AND time < '${endIso}'`,
      `AND lastSeen < '${markIso}'`,
    ].join(" ");

    try {
      if (this.traceIngest) {
        logger.info(`[trace][questdb] window_soft_delete run key=${key}`);
      }
      await withRetry(
        `QuestDB window soft-delete ${tableName}`,
        async () =>
          await this.execCircuit.execute(async () => {
            const res = await fetch(this.buildExecQueryUrl(query), { headers: execHeaders });
            if (!res.ok) {
              const text = await res.text();
              throw this.buildHttpError(`QuestDB window soft-delete failed`, res.status, text);
            }
          }),
        {
          attempts: 4,
          baseDelayMs: 200,
          maxDelayMs: 2500,
          shouldRetry: isRetryableNetworkError,
          onRetry: ({ attempt, delayMs, error }) => {
            logger.warn(
              `Window soft-delete retry for ${tableName} (attempt ${attempt}) in ${delayMs}ms: ${errorMessage(error)}`,
            );
          },
        },
      );
      this.clearedWindows.add(key);
    } catch (err: any) {
      // If soft-delete fails, continue inserting to avoid data loss; log for investigation.
      logger.error(`Failed to soft-delete window for ${tableName}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  private async runWithTable(
    tableName: string,
    kind: "data" | "table",
    ingestMode: IngestMode,
    columns: CanonicalTableColumnEntry[],
    writeFn: () => Promise<void>,
  ): Promise<void> {
    const previous = this.pendingWrites.get(tableName) ?? Promise.resolve();
    let writeCompletion: Promise<void> | undefined;
    const job = previous
      .catch(() => undefined)
      .then(async () => {
        await this.ensureTable(tableName, ingestMode, kind, columns);
        // Keep table setup/submission ordered, but do not make this table's
        // admission queue wait for the shared sender to flush. The batcher
        // itself serializes all row construction and bounds accepted rows.
        writeCompletion = this.safeWrite(writeFn);
      });

    // Store only setup/submission work so a slow flush cannot create an
    // unbounded per-table promise chain ahead of the bounded batcher.
    this.pendingWrites.set(tableName, job.catch(() => undefined));
    await job;
    if (!writeCompletion) {
      throw new Error(`QuestDB write for ${tableName} was not submitted.`);
    }
    await writeCompletion;
  }

  private safeWrite(writeFn: () => Promise<void>): Promise<void> {
    return this.batcher.enqueue(writeFn);
  }

  private async ensureTable(
    tableName: string,
    ingestMode: IngestMode,
    kind: "data" | "table",
    columns?: CanonicalTableColumnEntry[],
  ): Promise<void> {
    if (!this.questDbExecConfig) return;
    if (ingestMode !== "dedup" && ingestMode !== "window_replace") return;
    const execHeaders = this.questDbExecConfig.headers;
    if (this.createdTables.has(tableName)) return;
    if (this.creatingTables.has(tableName)) {
      await this.creatingTables.get(tableName);
      return;
    }

    const promise = (async () => {
      const ddl = kind === "data" ? this.buildDataTableDdl(tableName) : this.buildTableTableDdl(tableName, columns ?? []);
      if (!ddl) return;
      await withRetry(
        `QuestDB CREATE TABLE ${tableName}`,
        async () =>
          await this.execCircuit.execute(async () => {
            const res = await fetch(this.buildExecQueryUrl(ddl), { headers: execHeaders });
            if (!res.ok) {
              const text = await res.text();
              throw this.buildHttpError(`QuestDB DDL failed`, res.status, text);
            }
          }),
        {
          attempts: 4,
          baseDelayMs: 200,
          maxDelayMs: 2500,
          shouldRetry: isRetryableNetworkError,
          onRetry: ({ attempt, delayMs, error }) => {
            logger.warn(`QuestDB table creation retry for ${tableName} (attempt ${attempt}) in ${delayMs}ms: ${errorMessage(error)}`);
          },
        },
      );
      this.createdTables.add(tableName);
      logger.info(`Created QuestDB table ${tableName} with dedup keys (mode=${kind})`);
    })()
      .catch((err: any) => {
        const message = err instanceof Error ? err.message : String(err);
        logger.error(`Failed to create QuestDB table ${tableName}: ${message}`);
        throw err;
      })
      .finally(() => {
        this.creatingTables.delete(tableName);
      });

    this.creatingTables.set(tableName, promise);
    await promise;
  }

  private buildExecQueryUrl(sql: string): string {
    if (!this.questDbExecConfig) {
      throw new Error("QuestDB exec URL is not configured");
    }
    return `${this.questDbExecConfig.url}?query=${encodeURIComponent(sql)}`;
  }

  private buildHttpError(prefix: string, status: number, body: string): Error {
    const error = new Error(`${prefix} (${status}): ${body}`);
    (error as any).status = status;
    return error;
  }

  private isRetryableIlpError(error: unknown): boolean {
    if (error instanceof NonRetryableError) return false;
    if (isRetryableNetworkError(error)) return true;
    const message = errorMessage(error).toLowerCase();
    return (
      message.includes("socket") ||
      message.includes("connection") ||
      message.includes("timed out") ||
      message.includes("broken pipe") ||
      message.includes("retry")
    );
  }

  private quoteIdent(value: string): string {
    const safe = (value ?? "").toString().replace(/"/g, '""');
    return `"${safe}"`;
  }

  private buildDataTableDdl(tableName: string): string {
    const q = (s: string) => this.quoteIdent(s);
    const cols = [
      `${q("time")} TIMESTAMP`,
      `${q("topic")} SYMBOL`,
      `${q("attribute")} SYMBOL`,
      `${q("asset")} SYMBOL`,
      `${q("objectType")} SYMBOL`,
      `${q("objectId")} SYMBOL`,
      `${q("eventId")} SYMBOL`,
      `${q("valueType")} SYMBOL`,
      `${q("deleted")} BOOLEAN`,
      `${q("lastSeen")} TIMESTAMP`,
      `${q("deletedAt")} TIMESTAMP`,
      `${q("intervalStart")} TIMESTAMP`,
      `${q("intervalEnd")} TIMESTAMP`,
      `${q("createdAt")} TIMESTAMP`,
      `${q("expiresAt")} TIMESTAMP`,
      `${q("interval")} LONG`,
      `${q("value")} DOUBLE`,
      `${q("stringValue")} SYMBOL`,
      `${q("numberValue")} DOUBLE`,
      `${q("uom")} STRING`,
      `${q("foreignEventKey")} STRING`,
    ];
    return `CREATE TABLE IF NOT EXISTS ${q(tableName)} (${cols.join(",")}) TIMESTAMP(${q("time")}) PARTITION BY DAY DEDUP UPSERT KEYS(${[q("eventId"), q("topic"), q("attribute"), q("asset"), q("objectType"), q("objectId"), q("time")].join(",")})`;
  }

  private buildTableTableDdl(
    tableName: string,
    columns: CanonicalTableColumnEntry[],
  ): string | null {
    if (!Array.isArray(columns) || columns.length === 0) return null;
    const q = (s: string) => this.quoteIdent(s);
    const mapped: string[] = [];
    for (const [columnName, column] of columns) {
      if (!column?.type) continue;
      const name = q(columnName);
      const type = this.mapColumnType(column.type);
      mapped.push(`${name} ${type}`);
      if (column.uom) {
        mapped.push(`${q(`${columnName}_uom`)} STRING`);
      }
    }
    const cols = [
      `${q("time")} TIMESTAMP`,
      `${q("topic")} SYMBOL`,
      `${q("attribute")} SYMBOL`,
      `${q("asset")} SYMBOL`,
      `${q("objectType")} SYMBOL`,
      `${q("objectId")} SYMBOL`,
      `${q("eventId")} SYMBOL`,
      `${q("deleted")} BOOLEAN`,
      `${q("lastSeen")} TIMESTAMP`,
      `${q("deletedAt")} TIMESTAMP`,
      `${q("intervalStart")} TIMESTAMP`,
      `${q("intervalEnd")} TIMESTAMP`,
      `${q("createdAt")} TIMESTAMP`,
      `${q("expiresAt")} TIMESTAMP`,
      ...mapped,
    ];
    return `CREATE TABLE IF NOT EXISTS ${q(tableName)} (${cols.join(",")}) TIMESTAMP(${q("time")}) PARTITION BY DAY DEDUP UPSERT KEYS(${[q("eventId"), q("topic"), q("attribute"), q("asset"), q("objectType"), q("objectId"), q("time")].join(",")})`;
  }

  private mapColumnType(colType: string): string {
    const t = colType.toLowerCase();
    switch (t) {
      case "symbol":
        return "SYMBOL";
      case "varchar":
      case "string":
      case "char":
      case "uuid":
      case "ipv4":
      case "binary":
      case "long256":
        return "STRING";
      case "float":
      case "double":
        return "DOUBLE";
      case "byte":
      case "short":
      case "int":
      case "long":
        return "LONG";
      case "boolean":
        return "BOOLEAN";
      case "date":
      case "timestamp":
      case "timestamp_ns":
        return "TIMESTAMP";
      default:
        // Pass through geohash/decimal/array types as-is
        return colType;
    }
  }

  private resetSenderBuffer() {
    this.questDbOutput.reset();
  }

  // Legacy helpers removed: table ingestion requires explicit `columns` with types.
}
