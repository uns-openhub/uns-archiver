import { Client } from "pg";
import { z } from "zod";
import { secretValueSchema } from "@uns-kit/core/uns-config/secret-placeholders.js";
import type { IUnsPacket, IUnsTableColumns } from "@uns-kit/core";
import { createHash } from "node:crypto";

export const importId = z.string().regex(/^[a-z][a-z0-9-]{0,63}$/);
const column = z.string().regex(/^[A-Za-z_][A-Za-z_0-9]{0,63}$/);
const topic = z
  .string()
  .min(3)
  .max(512)
  .regex(/^[^/#+'\x00-\x20]+(?:\/[^/#+'\x00-\x20]+)+$/);
const destination = {
  sourceSeries: z.string().min(1).max(512),
  topic,
  dataGroup: column.optional(),
  before: z
    .string()
    .min(20)
    .max(27)
    .optional()
    .describe("Exclusive UTC cutoff for this mapping."),
};
const mappingSchema = z.discriminatedUnion("kind", [
  z.strictObject({
    ...destination,
    kind: z.literal("data"),
    column,
    type: z.enum(["number", "string"]),
    unit: z.string().min(1).max(40).optional(),
  }),
  z.strictObject({
    ...destination,
    kind: z.literal("table"),
    columns: z
      .array(
        z.strictObject({
          name: column,
          column,
          type: z.enum(["double", "string", "boolean"]),
        }),
      )
      .min(1)
      .max(64),
  }),
]);
const profileSchema = z.strictObject({
  id: importId,
  source: importId,
  sourceTable: column.describe(
    "The only WAL table read by the reviewed SELECT.",
  ),
  targetPrefix: z.string().regex(/^history_import_[a-z][a-z0-9_]{0,39}$/),
  query: z
    .string()
    .trim()
    .min(10)
    .max(16000)
    .regex(/^SELECT\s/i)
    .refine(
      (sql) => !/[;\u0000]/.test(sql) && /\$1\b/.test(sql) && /\$2\b/.test(sql),
      "Use one SELECT with bound lower and upper timestamp limits ($1 and $2).",
    ),
  mappings: z.array(mappingSchema).min(1).max(64),
});

export const historicalImportSettingsSchema = z.strictObject({
  sources: z
    .array(
      z.strictObject({
        id: importId,
        driver: z.literal("questdb"),
        connectionString: secretValueSchema,
      }),
    )
    .min(1)
    .max(16),
  profiles: z.array(profileSchema).min(1).max(32),
});

export type HistoricalImportSettings = z.infer<
  typeof historicalImportSettingsSchema
>;
export type ImportProfile = z.infer<typeof profileSchema>;
export type ImportMapping = z.infer<typeof mappingSchema>;
export type ImportCursor = { time: string; key: string };
export type SourceRow = {
  cursor: ImportCursor;
  series: string;
  columns: Record<string, unknown>;
};
export type HistoricalReader = {
  read: (
    profile: ImportProfile,
    from: string,
    to: string,
    after: ImportCursor | null,
    limit: number,
  ) => Promise<{ rows: SourceRow[]; seal: string }>;
};
export const MAX_PAGE_ROWS = 128;
const stableKey = /^[!-~]{1,512}$/;
const reservedSource = new Set([
  "archive_time",
  "archive_key",
  "archive_series",
  "__import_us",
]);
const reservedDestination = new Set([
  "timestamp",
  "time",
  "topic",
  "fulltopic",
  "eventid",
  "attribute",
  "asset",
  "objecttype",
  "objectid",
  "stableentityid",
  "entitytypekey",
  "identityresolution",
  "identitytimebasis",
  "identitybindingrevision",
  "identitybindingdigest",
  "deleted",
  "deletedat",
  "lastseen",
  "createdat",
  "expiresat",
  "interval",
  "intervalstart",
  "intervalend",
]);

export class ImportError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
  ) {
    super(code);
  }
}

export function utcMicros(input: string): string {
  const match = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?Z$/.exec(
    input,
  );
  if (!match) throw new ImportError(400, "invalid-utc-timestamp");
  const seconds = Date.parse(match[1] + "Z");
  if (
    !Number.isFinite(seconds) ||
    new Date(seconds).toISOString().slice(0, 19) !== match[1]
  ) {
    throw new ImportError(400, "invalid-utc-timestamp");
  }
  return (
    BigInt(seconds) * 1000n +
    BigInt((match[2] ?? "").padEnd(6, "0"))
  ).toString();
}

export function isoMicros(micros: string): string {
  const value = BigInt(micros);
  const millis = value / 1000n;
  const remainder = value % 1000n;
  return new Date(Number(millis))
    .toISOString()
    .replace(/Z$/, remainder.toString().padStart(3, "0") + "Z");
}

export function compareCursors(a: ImportCursor, b: ImportCursor): number {
  const delta = BigInt(a.time) - BigInt(b.time);
  if (delta !== 0n) return delta < 0n ? -1 : 1;
  return a.key < b.key ? -1 : a.key > b.key ? 1 : 0;
}

export function historicalSourceIdentity(connectionString: string): string {
  try {
    const url = new URL(connectionString);
    if (
      ["postgresql:", "postgres:"].includes(url.protocol) &&
      url.hostname &&
      url.username &&
      !url.hash &&
      [...url.searchParams.keys()].every((key) =>
        ["sslmode", "sslrootcert", "sslcert", "sslkey"].includes(key),
      )
    ) {
      return createHash("sha256")
        .update(JSON.stringify([url.host, url.pathname]))
        .digest("hex");
    }
  } catch {
    /* Never include a credential-bearing connection string in an error. */
  }
  throw new Error("Invalid historical source connection");
}

export function historicalWalTransaction(
  row: unknown[] | undefined,
  side: "source" | "target",
): string {
  if (!row) throw new ImportError(409, `historical-${side}-requires-wal`);
  const [suspended, writer, sequencer] = row;
  if (
    suspended !== false ||
    typeof writer !== "string" ||
    typeof sequencer !== "string" ||
    !/^-?\d+$/.test(sequencer) ||
    writer !== sequencer
  )
    throw new ImportError(503, `historical-${side}-wal-pending`);
  return sequencer;
}

export function questDbHistoricalReader(
  connectionString: string,
): HistoricalReader {
  historicalSourceIdentity(connectionString);
  return {
    async read(profile, from, to, after, limit) {
      const fields = [
        ...new Set(
          profile.mappings.flatMap((mapping) =>
            mapping.kind === "data"
              ? [mapping.column]
              : mapping.columns.map((item) => item.column),
          ),
        ),
      ].map((name) => '"' + name + '"');
      const lower = after?.time ?? from;
      const client = new Client({
        connectionString,
        connectionTimeoutMillis: 2500,
        query_timeout: 3500,
        statement_timeout: 3000,
        application_name: "uns-archiver-history",
      });
      // Prevent a late socket error (for example after a timed-out read) from exiting the Archiver.
      client.on("error", () => undefined);
      let rows: Record<string, unknown>[];
      let seal: string;
      const inspectSeal = async () => {
        const result = await client.query({
          text: `SELECT suspended, cast(writerTxn as varchar), cast(sequencerTxn as varchar)
            FROM wal_tables() WHERE name = cast($1 as varchar)`,
          values: [profile.sourceTable],
          rowMode: "array",
        });
        return historicalWalTransaction(result.rows[0], "source");
      };
      try {
        await client.connect();
        seal = await inspectSeal();
        ({ rows } = await client.query(
          `SELECT archive_key, archive_series, cast(archive_time as long) AS __import_us, ${fields.join(", ")}
           FROM (${profile.query})
           WHERE (archive_time > cast(cast($1 as long) as timestamp)
             OR (archive_time = cast(cast($1 as long) as timestamp) AND archive_key > cast($3 as varchar)))
           ORDER BY archive_time, archive_key LIMIT ${Math.min(limit + 1, MAX_PAGE_ROWS + 1)}`,
          [lower, to, after?.key ?? ""],
        ));
        if (seal !== (await inspectSeal()))
          throw new ImportError(409, "historical-source-changed");
      } catch (error) {
        if (error instanceof ImportError) throw error;
        throw new ImportError(503, "historical-source-unavailable");
      } finally {
        await client.end().catch(() => undefined);
      }

      const result: SourceRow[] = [];
      for (const row of rows) {
        const time = String(row.__import_us);
        const key = row.archive_key;
        const series = row.archive_series;
        if (
          !/^\d{1,18}$/.test(time) ||
          typeof key !== "string" ||
          !stableKey.test(key) ||
          typeof series !== "string" ||
          BigInt(time) < BigInt(from) ||
          BigInt(time) >= BigInt(to)
        ) {
          throw new ImportError(422, "invalid-historical-source-row");
        }
        const cursor = { time, key };
        const previous = result.at(-1)?.cursor ?? after;
        if (previous && compareCursors(previous, cursor) >= 0) {
          throw new ImportError(422, "non-unique-historical-source-cursor");
        }
        result.push({ cursor, series, columns: row });
      }
      return { rows: result.slice(0, limit), seal };
    },
  };
}

function mappedValue(
  columns: Record<string, unknown>,
  field: string,
  type: "number" | "string" | "boolean",
) {
  const value = columns[field];
  if (
    !Object.hasOwn(columns, field) ||
    typeof value !== type ||
    (typeof value === "number" && !Number.isFinite(value)) ||
    (typeof value === "string" && Buffer.byteLength(value) > 8192)
  ) {
    throw new ImportError(422, "invalid-historical-mapped-value");
  }
  return value as string | number | boolean;
}

export function packetForHistoricalRow(
  profile: ImportProfile,
  mapping: ImportMapping,
  row: SourceRow,
  jobId: string,
) {
  const eventId =
    "history:" +
    jobId +
    ":" +
    createHash("sha256")
      .update(
        JSON.stringify([
          profile.id,
          mapping.topic,
          mapping.kind,
          row.cursor.time,
          row.cursor.key,
        ]),
      )
      .digest("hex");
  const common = {
    time: isoMicros(row.cursor.time) as NonNullable<
      IUnsPacket["message"]["data"]
    >["time"],
    dataGroup: mapping.dataGroup,
    eventId,
  };
  let message: IUnsPacket["message"];
  if (mapping.kind === "data") {
    message = {
      data: {
        ...common,
        value: mappedValue(row.columns, mapping.column, mapping.type) as
          number | string,
        valueType: mapping.type,
        uom: mapping.unit as NonNullable<IUnsPacket["message"]["data"]>["uom"],
      },
    };
  } else {
    const columns: IUnsTableColumns = Object.fromEntries(
      mapping.columns.map((entry) => [
        entry.name,
        {
          type: entry.type,
          value: mappedValue(
            row.columns,
            entry.column,
            entry.type === "double" ? "number" : entry.type,
          ),
        },
      ]),
    );
    message = { table: { ...common, columns } };
  }
  const packet: IUnsPacket = { version: "2.0.0", message };
  return { packet, eventId };
}

export function historicalPrefixesOverlap(a: string, b: string): boolean {
  a = a.toLowerCase();
  b = b.toLowerCase();
  return a === b || a.startsWith(b + "_") || b.startsWith(a + "_");
}

export function assertHistoricalImportSettings(
  settings: HistoricalImportSettings,
) {
  const sources = new Set(settings.sources.map((source) => source.id));
  if (sources.size !== settings.sources.length)
    throw new Error("Duplicate historical source ID");
  const profiles = new Set<string>();
  const prefixes: string[] = [];
  for (const profile of settings.profiles) {
    if (!sources.has(profile.source) || profiles.has(profile.id))
      throw new Error("Invalid historical import profile");
    profiles.add(profile.id);
    if (
      prefixes.some((prefix) =>
        historicalPrefixesOverlap(prefix, profile.targetPrefix),
      )
    )
      throw new Error("Historical profiles must use separate target prefixes");
    prefixes.push(profile.targetPrefix);
    const targets = new Set<string>();
    const series = new Set<string>();
    for (const mapping of profile.mappings) {
      const key = JSON.stringify([
        mapping.kind,
        mapping.topic,
        mapping.dataGroup ?? "",
      ]);
      if (series.has(mapping.sourceSeries) || targets.has(key))
        throw new Error("Duplicate historical import mapping");
      const mappedFields =
        mapping.kind === "data"
          ? [mapping.column]
          : mapping.columns.map((item) => item.column);
      if (mappedFields.some((name) => reservedSource.has(name.toLowerCase())))
        throw new Error("Reserved historical source column");
      if (mapping.kind === "table") {
        const names = mapping.columns.map((item) => item.name.toLowerCase());
        if (
          new Set(names).size !== names.length ||
          names.some((name) => reservedDestination.has(name))
        )
          throw new Error("Invalid historical target column");
      }
      if (mapping.before && BigInt(utcMicros(mapping.before)) < 0n)
        throw new Error("Invalid historical mapping cutoff");
      series.add(mapping.sourceSeries);
      targets.add(key);
    }
  }
}
