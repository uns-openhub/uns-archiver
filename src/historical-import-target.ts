import {
  isoMicros,
  ImportError,
  historicalWalTransaction,
} from "./historical-import-source.js";

export type HistoricalTarget = {
  topic: string;
  table: string;
  prefix: string;
  kind: "data" | "table";
  identity: string;
  before?: string;
};
type ReadSql = (sql: string) => Promise<unknown[][]>;
const literal = (text: string) => "'" + text.replaceAll("'", "''") + "'";
const ident = (text: string) => '"' + text.replaceAll('"', '""') + '"';
const knownKeys = new Set([
  "eventId",
  "topic",
  "attribute",
  "asset",
  "objectType",
  "objectId",
]);

async function inspectWal(read: ReadSql, table: string): Promise<string> {
  const [row] =
    await read(`SELECT suspended, cast(writerTxn as varchar), cast(sequencerTxn as varchar)
    FROM wal_tables() WHERE name = ${literal(table)}`);
  return historicalWalTransaction(row, "target");
}

export async function inspectHistoricalTargets(
  read: ReadSql,
  targets: HistoricalTarget[],
  from: string,
  to: string,
  jobId?: string,
) {
  const groups = new Map<string, HistoricalTarget[]>();
  for (const target of targets) {
    const group = groups.get(target.table) ?? [];
    group.push(target);
    groups.set(target.table, group);
  }
  const result = new Map<
    HistoricalTarget,
    { timestampColumn: string; overlap: boolean; before: string }
  >();
  for (const [table, items] of groups) {
    const transaction = await inspectWal(read, table);
    const [[dedup] = []] = await read(
      `SELECT dedup FROM tables() WHERE table_name = ${literal(table)}`,
    );
    if (dedup !== true)
      throw new ImportError(409, "historical-target-requires-dedup");
    const columns = await read(
      `SELECT "column", type, designated, upsertKey FROM table_columns(${literal(table)})`,
    );
    const time = columns.find((row) => row[2] === true);
    const keys = columns
      .filter((row) => row[3] === true)
      .map((row) => String(row[0]));
    if (
      !time ||
      time[1] !== "TIMESTAMP" ||
      !keys.includes(String(time[0])) ||
      keys.length !== knownKeys.size + 1 ||
      ![...knownKeys].every((key) => keys.includes(key)) ||
      !columns.some((row) => row[0] === "eventId" && row[1] === "SYMBOL") ||
      !columns.some((row) => row[0] === "fullTopic")
    ) {
      throw new ImportError(409, "historical-target-incompatible-schema");
    }
    const timestampColumn = String(time[0]);
    const intervals = new Map<string, HistoricalTarget[]>();
    for (const item of items) {
      const cutoff =
        item.before && BigInt(item.before) < BigInt(to) ? item.before : to;
      const group = intervals.get(cutoff) ?? [];
      group.push(item);
      intervals.set(cutoff, group);
    }
    for (const [cutoff, group] of intervals) {
      for (let offset = 0; offset < group.length; offset += 12) {
        const chunk = group.slice(offset, offset + 12);
        const existing =
          BigInt(cutoff) <= BigInt(from)
            ? []
            : await read(
                `SELECT fullTopic FROM ${ident(table)} WHERE ${ident(timestampColumn)} >= ${literal(isoMicros(from))}
           AND ${ident(timestampColumn)} < ${literal(isoMicros(cutoff))}
           AND fullTopic IN (${chunk.map((item) => literal(item.topic)).join(", ")})
           ${jobId ? `AND (eventId IS NULL OR eventId NOT LIKE ${literal("history:" + jobId + ":%")})` : ""}
           GROUP BY fullTopic`,
              );
        const overlapping = new Set(existing.map((row) => String(row[0])));
        for (const item of chunk) {
          result.set(item, {
            timestampColumn,
            overlap: overlapping.has(item.topic),
            before: isoMicros(cutoff),
          });
        }
      }
    }
    if (transaction !== (await inspectWal(read, table)))
      throw new ImportError(503, "historical-target-wal-changed");
  }
  return targets.map((target) => ({ ...target, ...result.get(target)! }));
}

/** Count job rows on the same QuestDB primary that acknowledged ingestion. */
export async function verifyHistoricalVisibility(
  read: ReadSql,
  targets: HistoricalTarget[],
  from: string,
  to: string,
  jobId: string,
  expected: number,
): Promise<HistoricalTarget[]> {
  let rows = 0;
  let ids = 0;
  const verified: HistoricalTarget[] = [];
  for (const table of new Set(targets.map((target) => target.table))) {
    const transaction = await inspectWal(read, table);
    const [time] = await read(
      `SELECT "column" FROM table_columns(${literal(table)}) WHERE designated`,
    );
    if (typeof time?.[0] !== "string")
      throw new ImportError(409, "historical-target-incompatible-schema");
    const timestamp = ident(time[0]);
    const range = `FROM ${ident(table)}
       WHERE ${timestamp} >= ${literal(isoMicros(from))} AND ${timestamp} < ${literal(isoMicros(to))}
       AND eventId LIKE ${literal("history:" + jobId + ":%")}`;
    const [[count, distinct] = []] = await read(
      `SELECT count(), count_distinct(eventId) ${range}`,
    );
    if (!Number.isSafeInteger(count) || !Number.isSafeInteger(distinct))
      throw new ImportError(503, "historical-visibility-unavailable");
    if (Number(count) > 0) {
      const mapped = new Map(
        targets
          .filter((target) => target.table === table)
          .map((target) => [target.topic, target]),
      );
      const topics = await read(
        `SELECT fullTopic ${range} GROUP BY fullTopic LIMIT ${mapped.size + 1}`,
      );
      if (!topics.length)
        throw new ImportError(503, "historical-visibility-unavailable");
      for (const [topic] of topics) {
        const target =
          typeof topic === "string" ? mapped.get(topic) : undefined;
        if (!target)
          throw new ImportError(409, "historical-visible-topic-mismatch");
        verified.push(target);
      }
    }
    if (transaction !== (await inspectWal(read, table)))
      throw new ImportError(503, "historical-target-wal-changed");
    rows += Number(count);
    ids += Number(distinct);
  }
  if (rows !== expected || ids !== expected)
    throw new ImportError(409, "historical-visible-count-mismatch");
  return verified;
}
