import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import { Sender } from "@questdb/nodejs-client";
import { operatorOperation } from "@uns-kit/core/runtime/operator-api.js";
import {
  historicalImportOperations,
  historicalImportStatusOperations,
} from "../src/historical-import-api.js";
import { HistoricalImport } from "../src/historical-import.js";
import {
  historicalImportSettingsSchema,
  historicalWalTransaction,
  historicalPrefixesOverlap,
  isoMicros,
  utcMicros,
} from "../src/historical-import-source.js";
import { QuestDBWriter } from "../src/writers/questDbWriter.js";

test("historical import operations use the shared MCP read and reviewed command contract", () => {
  const [status] = historicalImportStatusOperations;
  const [preview, start] = historicalImportOperations;
  assert.equal(
    operatorOperation(status, "GET", "/archive/history-imports").kind,
    "read",
  );
  assert.equal(
    operatorOperation(preview, "POST", "/archive/history-import-control").kind,
    "read",
  );
  const command = operatorOperation(
    start,
    "POST",
    "/archive/history-import-control",
  );
  assert.equal(command.writeDomain, "history-import");
  assert.equal(command.replay, "request-id-v1");
  assert.equal(
    isoMicros(utcMicros("2024-02-29T10:15:01.123456Z")),
    "2024-02-29T10:15:01.123456Z",
  );
  assert.throws(
    () => utcMicros("2025-02-29T10:15:01Z"),
    /invalid-utc-timestamp/,
  );
});

test("a historical table prefix cannot collide with a live prefix plus data group", () => {
  assert.equal(
    historicalPrefixesOverlap("history", "history_import_fixture"),
    true,
  );
  assert.equal(
    historicalPrefixesOverlap(
      "history_import_fixture",
      "HISTORY_IMPORT_FIXTURE_METER",
    ),
    true,
  );
  assert.equal(
    historicalPrefixesOverlap("live_fixture", "history_import_fixture"),
    false,
  );
});

test("only fully applied, unsuspended WAL provides a usable transaction fence", () => {
  assert.equal(historicalWalTransaction([false, "42", "42"], "source"), "42");
  assert.throws(
    () => historicalWalTransaction([false, "41", "42"], "target"),
    /target-wal-pending/,
  );
  assert.throws(
    () => historicalWalTransaction([true, "42", "42"], "target"),
    /target-wal-pending/,
  );
});

const httpPort = process.env.HISTORY_TEST_HTTP_PORT;
const pgPort = process.env.HISTORY_TEST_PG_PORT;
test(
  "QuestDB backfill preserves microseconds across a restart, prevents overlap, and writes scalar and table values",
  {
    skip: !httpPort || !pgPort,
    timeout: 30000,
  },
  async () => {
    assert.match(httpPort!, /^\d+$/);
    assert.match(pgPort!, /^\d+$/);
    const sourceName = "history_fixture_" + randomUUID().replaceAll("-", "");
    const prefix = "history_import_fixture_" + randomUUID().replaceAll("-", "");
    const dir = await mkdtemp(join(tmpdir(), "archive-history-test-"));
    const addr = "127.0.0.1:" + httpPort;
    const endpoint = "http::addr=" + addr + ";auto_flush=off;";
    const settings = historicalImportSettingsSchema.parse({
      sources: [
        {
          id: "fixture",
          driver: "questdb",
          connectionString: `postgresql://user:quest@127.0.0.1:${pgPort}/qdb`,
        },
      ],
      profiles: [
        {
          id: "fixture-import",
          source: "fixture",
          sourceTable: sourceName,
          targetPrefix: prefix,
          query: `SELECT timestamp AS archive_time, row_key AS archive_key, series AS archive_series, value
        FROM ${sourceName}
        WHERE timestamp >= cast(cast($1 as long) as timestamp) AND timestamp < cast(cast($2 as long) as timestamp)`,
          mappings: [
            {
              sourceSeries: "scalar",
              topic: "fixture/asset/value",
              kind: "data",
              type: "number",
              column: "value",
              before: "2024-01-01T01:30:00Z",
            },
            {
              sourceSeries: "table",
              topic: "fixture/asset/rows",
              kind: "table",
              columns: [
                { name: "measurement", type: "double", column: "value" },
              ],
            },
            {
              sourceSeries: "empty",
              topic: "fixture/asset/empty",
              kind: "data",
              type: "number",
              column: "value",
            },
          ],
        },
      ],
    });
    let writer: QuestDBWriter | undefined;
    let journal:
      ReturnType<QuestDBWriter["configureTargetJournal"]> | undefined;
    let manager: HistoricalImport | undefined;
    const sql = async (query: string): Promise<any> => {
      const res = await fetch(
        `http://${addr}/exec?query=${encodeURIComponent(query)}`,
        { signal: AbortSignal.timeout(4000) },
      );
      const result = await res.json();
      assert.ok(res.ok, JSON.stringify(result));
      return result.dataset ?? [];
    };
    const awaitCondition = async <T>(
      work: () => Promise<T | undefined>,
    ): Promise<T> => {
      for (let attempt = 0; attempt < 90; attempt++) {
        const result = await work();
        if (result !== undefined) return result;
        await sleep(70);
      }
      throw new Error(
        "Timed out waiting for local archive: " +
          JSON.stringify(manager?.status()),
      );
    };
    try {
      await sql(`CREATE TABLE ${sourceName} (timestamp TIMESTAMP, row_key VARCHAR, series VARCHAR, value DOUBLE)
      TIMESTAMP(timestamp) PARTITION BY DAY WAL`);
      await sql(`INSERT INTO ${sourceName} VALUES
      ${Array.from({ length: 130 }, (_, i) => `('2024-01-01T01:00:00.123456Z', 'k${String(i).padStart(3, "0")}', 'scalar', ${i})`).join(",")},
      ('2024-01-01T01:00:00.123457Z', 'last', 'table', 321),
      ('2024-01-01T02:00:00.000000Z', 'after', 'scalar', 999)`);
      await awaitCondition(
        async () =>
          Number((await sql(`SELECT count() FROM ${sourceName}`))[0][0]) ===
            132 || undefined,
      );
      writer = new QuestDBWriter(await Sender.fromConfig(endpoint), endpoint, {
        flushIntervalMs: 30,
      });
      const time = "2026-01-01T00:00:00.000Z";
      await writer.writeUnsPacket(
        {
          version: "2.0.0",
          message: {
            data: {
              time: "2024-01-01T02:00:00.000Z",
              value: 777,
              valueType: "number",
            },
          },
        },
        prefix,
        "fixture/asset/value",
        undefined,
        "dedup",
      );
      await writer.writeUnsPacket(
        {
          version: "2.0.0",
          message: {
            table: {
              time,
              columns: { measurement: { type: "double", value: 555 } },
            },
          },
        },
        prefix,
        "fixture/asset/rows",
        undefined,
        "dedup",
      );
      journal = writer.configureTargetJournal({
        directory: join(dir, "targets"),
        policyFile: join(dir, "policy.json"),
        destinationId: "test",
      });
      journal.start();
      let failAfterWrite = true;
      let sourceNotifications = 0;
      const create = () =>
        new HistoricalImport(settings, {
          directoryFile: join(dir, "jobs.sqlite"),
          canWrite: () => true,
          resolveTarget: (mapping) => ({
            topic: mapping.topic,
            kind: mapping.kind,
            table: prefix + "_" + mapping.kind,
            prefix,
            identity: "fixture",
          }),
          readTarget: (query) => writer!.readHistoricalTarget(query),
          write: async (mapping, _, record, row) => {
            await writer!.writeUnsPacket(
              record.packet,
              prefix,
              mapping.topic,
              undefined,
              "dedup",
              undefined,
              {
                occurrenceId: record.eventId,
                legacyKey: record.eventId,
                eventTimeMicros: row.cursor.time,
                maxBatchWaitMs: 15,
              },
            );
            if (row.cursor.key === "k128" && failAfterWrite) {
              failAfterWrite = false;
              throw new Error(
                "Fixture interruption after database acknowledgement",
              );
            }
          },
          acknowledge: (id) => journal!.acknowledgeLegacy(id),
          onSourcesChanged: () => {
            sourceNotifications++;
          },
        });
      manager = create();
      const range = {
        profileId: "fixture-import",
        from: "2024-01-01T00:00:00Z",
        to: "2024-01-02T00:00:00Z",
      };
      const review = await manager.preview(range);
      assert.equal(review.canStart, true);
      assert.equal(review.sampleRows, 16);
      const request = {
        ...range,
        requestId: randomUUID(),
        previewDigest: review.previewDigest,
      };
      await assert.rejects(
        manager.start(
          { ...request, previewDigest: "0".repeat(64) },
          "operator",
        ),
        /preview-stale/,
      );
      assert.equal(
        (await manager.start(request, "operator")).id,
        request.requestId,
      );
      assert.throws(() => manager!.assertHandoverReady(), /must complete/);
      await awaitCondition(
        async () =>
          manager!.status().jobs[0]?.reason === "historical-io-retry" ||
          undefined,
      );
      assert.equal(manager.status().jobs[0].written, 128);
      assert.deepEqual(manager.publishedSources(), []);
      assert.equal(sourceNotifications, 0);
      await manager.close();
      manager = create();
      assert.equal(
        (await manager.start(request, "operator")).id,
        request.requestId,
      );
      manager.kick();
      await awaitCondition(
        async () =>
          manager!.status().jobs[0]?.state === "completed" || undefined,
      );
      assert.equal(manager.status().jobs[0].written, 131);
      assert.equal(manager.status().jobs[0].skippedAfterCutoff, 1);
      assert.doesNotThrow(() => manager!.assertHandoverReady());
      const sources = [
        {
          topic: "fixture/asset/rows",
          tableName: prefix + "_table",
          tablePrefix: prefix,
          dataGroup: null,
          suffix: "_table",
          identity: "fixture",
        },
        {
          topic: "fixture/asset/value",
          tableName: prefix + "_data",
          tablePrefix: prefix,
          dataGroup: null,
          suffix: "_data",
          identity: "fixture",
        },
      ];
      assert.deepEqual(manager.publishedSources(), sources);
      assert.equal(sourceNotifications, 1);
      await manager.close();
      manager = create();
      assert.deepEqual(manager.publishedSources(), sources);
      assert.equal(sourceNotifications, 1);

      // Simulate a completed job stored by the previous version, before it
      // recorded sources. Recovery must verify the existing rows, not rewrite them.
      await manager.close();
      const previous = new DatabaseSync(join(dir, "jobs.sqlite"));
      try {
        previous.exec("DELETE FROM history_sources");
        previous
          .prepare(
            "UPDATE imports SET job = json_remove(job, '$.sourcesRecorded') WHERE id = ?",
          )
          .run(request.requestId);
      } finally {
        previous.close();
      }
      manager = create();
      assert.deepEqual(manager.publishedSources(), []);
      manager.kick();
      await awaitCondition(
        async () => manager!.publishedSources().length === 2 || undefined,
      );
      assert.deepEqual(manager.publishedSources(), sources);
      assert.equal(sourceNotifications, 2);

      const data = await awaitCondition(async () => {
        const result = await sql(
          `SELECT value, cast(time AS LONG) FROM ${prefix}_data WHERE time < '2024-01-01T01:30:00Z' ORDER BY value`,
        );
        return result.length === 130 ? result : undefined;
      });
      assert.deepEqual(
        data,
        Array.from({ length: 130 }, (_, i) => [
          i,
          Number(utcMicros("2024-01-01T01:00:00.123456Z")),
        ]),
      );
      assert.deepEqual(
        await sql(
          `SELECT measurement, cast(time AS LONG) FROM ${prefix}_table WHERE time < '2025-01-01'`,
        ),
        [[321, Number(utcMicros("2024-01-01T01:00:00.123457Z"))]],
      );
      assert.deepEqual(
        await sql(
          `SELECT value FROM ${prefix}_data WHERE time >= '2024-01-01T01:30:00Z'`,
        ),
        [[777]],
      );
      // A pre-existing live append table retains distinct records at the same
      // key, while enabling dedup on the same shape collapses NULL event IDs.
      for (const mode of ["append", "dedup"] as const) {
        const livePrefix = sourceName + "_live_" + mode;
        for (const value of [1, 2]) {
          await writer.writeUnsPacket(
            {
              version: "2.0.0",
              message: { data: { time, value, valueType: "number" } },
            },
            livePrefix,
            "fixture/live/value",
            undefined,
            mode,
          );
        }
        const expected = mode === "append" ? 2 : 1;
        const result = await awaitCondition(async () => {
          const result = await sql(
            `SELECT value FROM ${livePrefix}_data ORDER BY value`,
          );
          return result.length === expected && result.at(-1)?.[0] === 2
            ? result
            : undefined;
        });
        assert.deepEqual(result, mode === "append" ? [[1], [2]] : [[2]]);
      }
      const overlap = await manager.preview(range);
      assert.equal(overlap.canStart, false);
      assert.deepEqual(
        overlap.targets.map((target) => target.overlap),
        [true, true, false],
      );
      await assert.rejects(
        manager.start(
          {
            ...range,
            requestId: randomUUID(),
            previewDigest: overlap.previewDigest,
          },
          "operator",
        ),
        /overlapping/,
      );
    } finally {
      await manager?.close();
      await journal?.close();
      await writer?.close();
      await rm(dir, { recursive: true, force: true });
    }
  },
);
