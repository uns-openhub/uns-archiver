# Historical database import

Historical database import is an opt-in Archiver operation. It reads a
configured database and writes historical packets through the same retained
target journal and QuestDB writer as ordinary archiving. The database source is
separate from MQTT and from the sealed legacy spool importer.

## Configure a reviewed profile

Configure `archiver.historicalImport` at startup. Reference an account with
read-only access to the source. Use the normal secret resolution for the source
connection string. Only the server configuration can set the source, SQL,
mappings, and optional exclusive UTC cutoffs.

Example shape using a fictional source and UNS topic:

```json
{
  "sources": [{
    "id": "archive-read",
    "driver": "questdb",
    "connectionString": "postgresql://readonly@localhost:8812/qdb"
  }],
  "profiles": [{
    "id": "historical-measurements",
    "source": "archive-read",
    "sourceTable": "retired_history",
    "targetPrefix": "history_import_measurements",
    "query": "SELECT timestamp AS archive_time, topic AS archive_series, topic AS archive_key, value FROM retired_history WHERE timestamp >= cast(cast($1 as long) as timestamp) AND timestamp < cast(cast($2 as long) as timestamp) AND topic = 'fixture/site/temperature'",
    "mappings": [{
      "sourceSeries": "fixture/site/temperature",
      "topic": "fixture/site/temperature-sensor/supply/temperature",
      "dataGroup": "measurements",
      "kind": "data",
      "type": "number",
      "column": "value",
      "unit": "°C",
      "before": "2025-01-01T00:00:00Z"
    }]
  }]
}
```

The single SELECT takes inclusive lower and exclusive upper timestamp
parameters in integer UTC microseconds. Return a microsecond
`TIMESTAMP` as `archive_time`, an ASCII `archive_key` that is unique at each
timestamp, and the mapping selector as `archive_series`. If the source can
have multiple rows with the same topic and time, return a stable unique row
key. The SELECT must read only the declared WAL `sourceTable`. Review the SQL
and keep the source table sealed until the import finishes. Archiver checks
that the source WAL is applied before and after reading a page and blocks the
job if the source transaction changes after approval. `kind: "table"` maps existing numeric,
string, or Boolean target columns through `columns: [{ "name", "type", "column" }]`.

## Preview and run

- Read `history-import.status` to list configured profiles and recent jobs.
- Call `history-import.preview` with `{ "action": "preview", "profileId", "from", "to" }`.
  The range is half-open UTC and is limited to 31 days per job. Preview checks
  a bounded source sample and checks the selected destination intervals for
  overlap. It returns the resolved final tables, cutoffs and a review digest.
- Start with `history-import.start` using the same inputs, `action: "start"`,
  `previewDigest`, and a new UUID `requestId`. The server refreshes the
  preview before accepting the job. Reuse the same UUID and payload after
  losing the response. The job receipt confirms acceptance. Status reports
  acknowledged rows, skipped rows after the configured cutoffs and progress.

The equivalent authenticated Archiver GET and POST operations use the
`history-imports` and `history-import-control` attributes. Controller MCP
discovers the declared read and reviewed command operations when supported
and authorized by the serving Controller and managed launch.

The topic must be confirmed by the Controller registry. A registered data
group can resume immediately after a process restart. A group known only
from MQTT observations waits for a fresh observation after restart. The worker
writes to a dedicated `history_import_…` prefix configured for the profile.
Archiver rejects prefixes that could resolve to a live storage table, even
after adding the data group. Provision the historical tables separately with
the normal writer's columns, `fullTopic`, a designated microsecond timestamp
and WAL deduplication on the timestamp, `eventId`, `topic`, `attribute`,
`asset`, `objectType`, and `objectId`. Grant one importing instance exclusive
write access to those targets during the job. Leave the existing live tables'
deduplication policy in place. Live events without an explicit ID or interval
may have NULL `eventId`, so changing a live append table to deduplication can
collapse distinct rows at the same timestamp.

Preview checks the primary's WAL and rejects existing history inside the
selected range. Configure a reviewed exclusive cutoff for each signal whose
live history begins at a different time. A timestamp alone may be insufficient
at a seam if the old and new feeds timestamp the same occurrence differently.

One job runs per Archiver instance under its existing target journal lease.
The worker uses the normal QuestDB writer with a deterministic historical
`eventId` per source row and job. The SQLite cursor advances after QuestDB
acknowledgement and is kept with the retained archive state. The worker yields
to live ingestion, honors replay pauses, and resumes the same job after restart.
A blocked job retains its checkpoint and prevents handover. Keep that state
together with the target journal for recovery.

A job completes once the primary has applied the WAL and the counts of rows
and distinct historical IDs match the checkpoint. Archiver records the
mappings for topics with verified rows and publishes them to the Controller
as history sources. It republishes those mappings from the retained archive
state after restart. Explore and Catalog can read the imported table together
with the live source when their schemas and QuestDB destination are compatible.

Use the same designated timestamp column name and measurement column types as
the live table. Query the intended reader separately to verify visibility,
microseconds, values, units and totals. On upgrade, Archiver can register
earlier completed jobs if the original profile still matches and the
destination still contains the verified rows.
