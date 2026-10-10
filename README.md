# UNS Archiver

UNS Archiver is a [UNS OpenHub](https://github.com/uns-openhub) add-on that persists
UNS data and table packets to QuestDB. It discovers active topics through the
controller, subscribes to configured MQTT topic filters, and publishes the resulting
QuestDB table mappings back to the UNS infrastructure.

## Features

- Archives scalar data and table packets to QuestDB.
- Supports `append`, `dedup`, and soft-delete `window_replace` ingestion modes.
- Derives stable QuestDB identities from UNS topic metadata.
- Buffers early or transiently failed events on disk and retries them.
- Publishes QuestDB mapping and dependency-health metadata.
- Reports durable ingest backlog through controller service health, so delayed history is visible even when QuestDB itself is reachable.
- Exposes authenticated control endpoints for status, pause, resume, and topics.

## Requirements

- Node.js 22 or newer
- pnpm 10
- A running UNS OpenHub controller and MQTT broker
- QuestDB with HTTP line protocol enabled

## Configuration profiles

The service ships three topology-specific profiles. They contain no deployment
credentials or customer endpoints.

| Profile                          | Use it when                                                  | MQTT and QuestDB                            | Service credential                          |
| -------------------------------- | ------------------------------------------------------------ | ------------------------------------------- | ------------------------------------------- |
| `config-development-host.json`   | Running the service directly with `pnpm run dev` on the host | `localhost`                                 | `UNS_SERVICE_TOKEN` from untracked `.env`   |
| `config-development-podman.json` | Deploying through a local Podman OpenHub controller          | Compose DNS: `mosquitto`, `questdb`         | Controller-managed `UNS_SERVICE_TOKEN_FILE` |
| `config-production.json`         | Creating a production controller instance                    | Compose/Runtime DNS: `mosquitto`, `questdb` | Controller-managed `/run` token file        |

The Podman and production profiles intentionally share their internal network
names: in both cases the RTT process runs alongside the controller. The
production profile sets `uns.env` to `prod` and is only a safe starting point;
the controller copies it into a per-instance configuration that is retained
across add-on releases.

## Direct host development

```bash
pnpm install
cp config-development-host.json config.json
cp .env.example .env
# Set UNS_SERVICE_TOKEN in .env to a development machine token.
pnpm run dev
```

For a controller-managed local Podman or production installation, deploy the
add-on from **Micro services** and select the matching profile. Do not copy the
repository `.env` into that instance. `input` inherits the full MQTT connection
from `infra`, so it is unnecessary unless it intentionally overrides a broker
setting.

Controller authentication resolves in this order: the controller-managed
`UNS_SERVICE_TOKEN_FILE`, direct-development `UNS_SERVICE_TOKEN`, `uns.token`, then
the legacy `uns.email`/`uns.password` fallback. The first three options avoid storing
a user password in `config.json`; use the legacy fallback only to bootstrap or replace
a development machine token. None of the committed profiles requires an email or
password.

When the controller starts an RTT instance, its injected
`UNS_CONTROLLER_PUBLIC_BASE` overrides the static `uns.rest` and `uns.graphql`
host for controller calls. This keeps one portable instance configuration usable
on every cluster member and ensures service registration is sent to the controller
that issued the instance credential. Direct repository starts continue to use the
configured URLs.

The control API must use either `uns.jwksWellKnownUrl` or `UNS_API_JWT_SECRET`.
JWKS is preferred when the archiver runs alongside UNS OpenHub.

### Ingest backpressure

`archiver.ingestQueueMaxEvents` (default `512`) and
`archiver.ingestQueueMaxBytes` (default `16777216`, 16 MiB) bound live MQTT
payloads while QuestDB is slow. Excess messages are synchronously persisted to
`./event_storage` and replayed in bounded fair batches while reserving 25% of
the live queue for MQTT traffic; they are not kept in an unbounded in-memory
promise backlog. `archiver.ingestConcurrency` defaults to `512`. Each live
worker waits until its QuestDB batch is flushed before taking another event,
so a single worker with a one-second flush interval can process only about
one event per second. Keep the queue and concurrency bounded, then measure
QuestDB throughput and process memory when tuning for a particular workload.

The active-topic registry is a separate history-eligibility gate. Packets for a
topic that is not yet active are held in memory only for
`archiver.inactiveBufferMaxAgeMs` and up to `archiver.inactiveBufferMax` events
to cover a short metadata race. If the topic is still inactive, the packet is
discarded rather than added to `event_storage`; a broad MQTT filter must not
turn unrelated infrastructure telemetry into an endless durable backlog.
Legacy spool entries marked `inactive_expired` or `inactive_overflow` are
acknowledged while replaying for the same reason.

`archiver.storedReplayBatchSize` defaults to `256`, and
`archiver.storedReplayIntervalMs` defaults to 500 ms. A pass processes
up to that many files concurrently so their
writes share the existing QuestDB ILP batcher. With single-row events and no
live traffic joining the same sender, this is roughly one flush per 64 replayed
events instead of one flush per event; table shape, live traffic, and
`questdb.batch.maxRows` determine the actual ratio. Replay concurrency is
derived from live capacity (up to one eighth of `ingestQueueMaxEvents`) and
never starts while the live queue has consumed its 25% reserve.

### Responsive spool status and recovery

MQTT startup and control responses do not wait for a full directory count or
orphan recovery. Inventory and stale `.processing` recovery run in the background;
replay, inventory and recovery each retain their own streaming directory cursor
between passes. Each pass visits at most **512 entries** (including nonmatching
names and directories) and checks a **25 ms work budget** before the next read.
The current filesystem operation cannot be forcibly cancelled, so a stalled read
can exceed that budget; it still does not block a cached HTTP status response.
Only the selected replay batch is retained in memory. All cursors close on normal
archiver shutdown. Restart begins a fresh background traversal; there is no
portable, persisted filesystem cursor in this release.

Maintenance continues incomplete scans every 100 ms. After a complete traversal,
inventory waits 5 seconds and recovery 30 seconds before starting another sweep.
This prevents every health poll or API caller from reopening a large directory.
The current instance's PID locks and live PIDs remain protected during automatic
recovery; PID reuse is not resolved by this change. Conflicting `.event` and
`.processing` files are preserved for inspection rather than silently discarding
one. Legacy-directory import and the candidate MQTT handover drain hook are
described below; the hook requires the new SDK and separate release acceptance.

Authenticated `/control` status, pause and resume return immediately from cached
inspection. Resume schedules replay without waiting for QuestDB to flush. Pause
keeps its existing ingestion semantics: new live events go to durable storage and
new replay work is stopped; it is not a separate legacy-import pause. A running
writer may still finish after the acknowledgement.

Both `/control` and `/topics` include:

- `queuedEvents`: cached observed count or lower bound; `null` when unavailable,
  or when a partial scan has found zero (which cannot prove an empty queue).
- `queuedEventsCountKind`: `unknown`, `lower-bound`, or `observed`.
- `queueInspection`: separate `queuedEvents`, `processingFiles`, `unresolvedProcessingFiles`, `otherEntries`,
  `entriesVisited`, `scanComplete`, `startedAt`, `capturedAt`, and sanitized
  `lastError` fields.
- `/topics` also includes `storedReplay.queueInspection` and
  `storedReplay.processingRecovery`, with existing replay/writer counters.

`observed` means a traversal reached its end, **not** an atomic count: writes,
recovery and replay can change the directory during the scan. A lower bound is
also an observation over the stated scan window, not a guaranteed current depth
once those files have been replayed. Use timestamps and inspection progress when
interpreting counts. An inaccessible or missing directory is `unknown`, never a
successful empty scan. A zero `.event` count does not cover `.processing` or other
unfinished files. Event contents, names and owner PIDs are not exposed in status.

The controller ingest-health signal uses this cached inspection every 30 seconds.
A backlog at or above 1,000 observed queued files, incomplete/stale inspection,
or unresolved processing files reports unconfirmed/degraded archive freshness.
Processing files currently owned by this replay's active batch do not degrade
health on their own. Other processing files may belong to another process or
interrupted replay; the signal asks the operator to inspect that distinction
instead of declaring the disk queue empty. A healthy
QuestDB connection alone does not prove recent history has been archived.

The `config-development-podman.json` profile is tuned for the bundled local
Podman runtime: it uses a 1024-event/32 MiB live queue, 128 concurrent ingest
operations, and a 250 ms QuestDB batch flush. Its replay pass handles up to
256 durable events every 500 ms while preserving live-queue headroom. The
host and production profiles use the measured 512-worker, 512-row batch
combination. Existing controller instances retain their own copied config
across releases, so review and update that instance config when upgrading
from an older profile.

### Import a sealed legacy spool after an upgrade

The new instance continues MQTT capture into its own `event_storage`. Importing
the old instance's queue is a **separate, explicitly started job**. It does not
move the entire queue into the live spool or change `/control` semantics.

For a synchronized cluster instance, provision **`legacy-import-sources.json`**
in that instance's working directory before starting it. It contains only the
local allowlist:

```json
[
  {
    "id": "retired-instance",
    "directory": "/srv/openhub/retired/archiver/event_storage"
  }
]
```

This optional startup file is limited to 64 KiB and 16 unique source IDs. Symlinks,
hardlinks, invalid JSON and unknown fields are rejected. Invalid local provisioning
stops startup; it never silently enables a different source. It is not a declared
portable runtime file and must be provisioned separately on the owning node.
No controller is required to read it. Changing it requires an archiver restart.

A standalone, unsynchronized instance may alternatively keep the allowlist in
its local startup configuration:

```json
{
  "archiver": {
    "legacySources": [
      {
        "id": "retired-instance",
        "directory": "/srv/openhub/retired/archiver/event_storage"
      }
    ],
    "legacyImport": {
      "batchSize": 256,
      "concurrency": 64,
      "maxFileBytes": 1048576,
      "maxBatchBytes": 16777216,
      "intervalMs": 100
    }
  }
}
```

Source IDs are unique; paths must be absolute canonical directories without
symlinks and cannot overlap the current live spool or another source. At most
16 sources are allowed. This allowlist and the import limits are read at startup:
restart the archiver after changing them. The service does not publish these
physical paths over MQTT. Keep them out of portable/shared configuration
templates. The companion controller portability guard rejects nonempty `legacySources` in synchronized
profiles, including provider references. Do not configure both this field and the
local provisioning file. The source and job must remain on the same host during
recovery. Older controllers do not enforce this explicit field restriction.

After the old MQTT/replay process has exited, use its preserved source ID:

- `GET /<processName>/api/system/archiver/service/<processName>/imports` returns
  cached source inspection, job counters, local ownership, quarantine presence,
  measured drain rates and the oldest event time **seen**, not an exact oldest
  queued event or ETA.
- `GET .../imports?action=inspect&sourceId=retired-instance` schedules a background
  inspection. Counts are `unknown`, `lower-bound`, or `observed`, with scan progress
  and capture time; an unavailable directory is never an empty queue.
- `POST .../import-control` accepts the following JSON body. Change `action` to
  `pause`, `resume`, or `cancel` and use the current job revision for subsequent
  commands. Start/resume require `confirmSourceClosed: true`.

```json
{
  "action": "start",
  "sourceId": "retired-instance",
  "requestId": "import-request-001",
  "expectedRevision": 0,
  "confirmSourceClosed": true
}
```

Both endpoints use the configured JWT/JWKS verification and token path grants.
`/topics` also includes cached legacy import status. Configured unfinished recovery
is published as a separate degraded dependency signal, so an empty new live spool
does not hide an old backlog in the controller's service health. No configured
legacy sources means this extra dependency signal is absent.
The mutation endpoint has a separate path: permission to read `imports` does
not permit `import-control`. API commands reject extra fields and arbitrary paths.
The same most recent request ID and identical body are idempotent; changed reuse
or a stale revision returns `409`. Revisions describe operator commands, while
progress counters update independently.

**Closure confirmation is an operator attestation**, not an automatic check of
the old process or a handover receipt. Never start import while the old process
can still write/replay the source. The source-local filesystem claim excludes
other importers; it cannot fence a legacy binary that does not understand it.
Ownership records use a random token and runtime instance identity. A dead owner
can be reclaimed only on explicit start/resume on the same hostname, after a
process liveness check. Live/reused PIDs and foreign-container owners block takeover
and require local review; there is no API force-unlock. Incomplete/corrupt claim
metadata also requires local review.

Import cursors visit at most 512 entries per pass and check a 25 ms work budget;
selected events and bytes are bounded separately. Default concurrency is capped
at one eighth of live queue capacity. Live headroom is checked before each write
chunk; a pending chunk may finish. Writer deferral backs off for at least five
seconds and resumes scanning beyond that file. The existing QuestDB sender is
shared with live ingestion. Pause acknowledges local state without waiting for a
pending database write, but it does persist a small checkpoint. Cancel and normal
shutdown wait for pending writes; they preserve unacknowledged files. Cancel does
not roll back rows already written and permits a later explicit restart of the job.

`.event` and recognized `.event.<pid>.<time>.processing` entries are read in place
after source closure. A source file is unlinked only after a confirmed shared ILP
flush or a known identical event already confirmed by this import runtime under
the same storage policy. The cache is bounded and not durable. Original event
time, data groups and supported 1.x/2.0 packet shapes are retained. Current active
topic membership is **not** used to discard valid historical events. Import still
requires a matching storage rule. Effective `window_replace` is deferred for
review because historical replay must not soft-delete newer data. If optional
identity enrichment cannot reach the controller, the persisted job start time
bounds the retry grace period; it does not restart on each file read.

Malformed/unsupported/oversized packets, unmatched storage and regular `.tmp` /
`.updated` / unknown files are preserved under the source-local
`.uns-archiver-import/quarantine/` with a reason. An unchanged real empty `failed/` directory created by old archivers is
tolerated without deletion. Nonempty or linked `failed/`, symlinks, hard links
and unexpected subdirectories remain untouched for review. Source contents, file names, paths,
owner PIDs, payloads and credentials are not returned by the import API.

Checkpoints and claims live in `.uns-archiver-import` inside the old source, using
atomic writes and filesystem sync, without a PostgreSQL registry or a per-event
database ledger. A fresh non-mutating verification traversal is required before
completion. Preserved quarantine prevents `completed`, even if a crash lost the
last counter update. After restart, jobs require explicit resume; traversal starts
again in bounded background passes. Interrupted DB acknowledgements may produce
duplicates, and counters can lag an interrupted batch. This is not exactly-once
delivery. A filesystem read in progress cannot be forcibly cancelled, so the
25 ms budget is not a hard IO deadline.

Do not uninstall the old version or remove its source volume until import has
finished and preserved files have been reviewed. Controller UI integration,
automatic handover receipt verification, migration/uninstall guards and a real
Runtime/QuestDB upgrade drill remain separate acceptance work.

For a neutral local file test with the real importer and QuestDB writer but a
synthetic sender, run `node --import ./node_modules/tsx/dist/loader.mjs
scripts/inspect-legacy-import.mts 20000`. It creates and deletes only its own
temporary fixture. Its results are not a production database throughput estimate.

For opt-in acceptance against a **disposable local QuestDB at 127.0.0.1:9000**:

```sh
pnpm -s exec tsx scripts/accept-real-questdb-import.mts 20000 /tmp/import-20000.json
# Bounded larger fixture, maximum 100,000 files:
pnpm -s exec tsx scripts/accept-real-questdb-import.mts 100000 /tmp/import-100000.json
```

The runner uses the production importer and writer with real HTTP ILP, a local
fault proxy, a randomly named fixture table with the existing `time` column
contract, and synthetic `.event` / abandoned `.processing` files. It checks pause
during a slow flush, rejected database writes, explicit resume after restart,
and every final database row's identity and original timestamp. ILP acknowledgement
and query visibility are measured separately. Status timings cover the cached
helper, not an authenticated API or browser. It removes its own table/files;
it does not exercise MQTT handover or the full 5.2.17 upgrade and must not be
reported as production throughput or exactly-once delivery.

## Configuration

The complete configuration contract is documented in
[`config.schema.json`](./config.schema.json). A storage rule maps a topic filter to a
QuestDB table prefix:

```json
{
  "tablePrefix": "uns_enterprise",
  "topic": "enterprise/#",
  "ingestMode": "dedup"
}
```

Existing installations may keep `questdb.configurationString`. New production
instances should use separate `questdb.url`, `questdb.username`, and
`questdb.password` values so credentials can be resolved independently from a
secret manager. The Archiver builds the QuestDB ILP connection in memory and
publishes only the credential-free endpoint in its table-mapping metadata.

### QuestDB ILP batching

The Archiver owns QuestDB ILP transaction boundaries and forces the underlying
sender to `auto_flush=off`. This avoids a separate HTTP/WAL transaction for
each archived row while keeping every `writeUnsPacket()` promise pending until
the batch containing that row has successfully flushed.

The committed profiles start with this bounded configuration:

```json
"batch": {
  "flushIntervalMs": 1000,
  "maxRows": 256,
  "maxPendingRows": 2048
}
```

At the observed steady rate of about 78 rows/s, a one-second interval normally
reduces roughly 6.77 million daily row-level transactions to about 86,400 batch
transactions (about 98.7% fewer). Traffic bursts flush at `maxRows`; a full
`maxPendingRows` queue rejects the write so the existing archiver error path
persists the event to `event_storage` rather than growing memory or marking it
archived. These values should be measured before increasing them.

`config.json`, `.env`, the event queue, and active-topic cache are intentionally
ignored by Git.

## Development

```bash
pnpm run verify
```

This runs the unit tests, TypeScript typecheck, and clean production build.

Additional scripts:

```bash
pnpm run generate-config-schema
pnpm run generate-codegen
pnpm run refresh-uns
```

The generated `UnsTopics` and `UnsTags` types are intentionally generic in this
repository. Run `refresh-uns` only against an environment whose topic and tag
metadata you are comfortable writing into your working tree.

## Releases

The package version is the source of truth. A release tag must be exactly
`v<package.json version>`; the release workflow validates the tag and runs the full
verification suite. This repository does not publish a package automatically.

## Security

Do not commit deployment configurations, credentials, generated environment
metadata, or buffered events. See [SECURITY.md](./SECURITY.md) for reporting
vulnerabilities.

## License

[MIT](./LICENSE) © Aljoša Vister.

### Application drain during handover (unreleased candidate)

The candidate requires `@uns-kit/core` 3.0.23 and registers its
`handoverShutdown` hook before starting the input proxy. Once an active source
accepts a handover, it stops starting new replay writes and subscription updates.
Already accepted live work continues. After the target acknowledges MQTT
ownership, the source awaits input proxy shutdown, live ingestion/durable spills,
current replay and legacy-import checkpoints, then closes the shared QuestDB
writer. SIGINT/SIGTERM use the same idempotent drain.

The deadline after acknowledgement is 30 seconds. Failure or timeout exits with
status 1 and an incomplete-drain diagnostic; successful drain exits with 0.
Persisted unacknowledged import files remain recoverable and restart requires
reviewed resume. Timeout can interrupt accepted in-memory live work and cannot
prove that a database write was cancelled. Delivery is not exactly once.
Review supervisor restart behavior before rollout: a failed old instance must
not automatically restart and reclaim ownership while the new one is active.

This hook is not retroactive: an older source binary still uses its installed
SDK's exit behavior. MQTT `handover_fin`/`handover_ack` are ownership-transfer
messages, not application-drain receipts. Confirm actual old-process exit before
starting legacy import. Real broker/QuestDB/Runtime upgrade acceptance remains
separate from the candidate's subprocess recovery tests.

### Operator import status

Authenticated `imports?format=operator` returns protocol 1, a nonsecret launch
identity, runtime selectors and cached source status. The controller import dialog
compares these selectors with the selected instance and sends `expectedOwnerId`
on commands and explicit inspection requests. A move/restart rejects stale
commands; released runtimes reject import control. Scripts may still use the
original array status and command contract. `expectedRevision` and `requestId`
remain required. Source closure remains an operator review, not a historical
MQTT acknowledgement. No source path or credential is returned in this status.

### Large directory acceptance and limits

The [local handover/import drill](docs/archiver-handover-import-acceptance.md)
uses actual 5.2.17 and candidate runtimes, real MQTT/QuestDB, authenticated
API/UI controls and an independent timestamp/ID oracle. Production upgrade
and 50-million-real-file acceptance remain open.

For scanner-only algorithm bounds, run:

```sh
node --import ./node_modules/tsx/dist/loader.mjs scripts/accept-large-directory-scan.mts 50000000 /tmp/scanner-50m.json
```

This generates names lazily, without physical files or database writes. Its
runtime cannot estimate physical spool import duration. A 25 ms scan budget
is cooperative; an individual disk operation may take longer.

### Bounded legacy import throughput

Legacy writes request a 25 ms maximum queue wait for their shared partial ILP
batch. The normal live `questdb.batch` configuration remains in effect, and the
hint only shortens a deadline: a slow in-flight flush can still delay the next
one. Row promises resolve after a real flush acknowledgement, not after the
timer fires. Default legacy pass spacing is 100 ms; explicit startup settings
remain authoritative. File/byte/concurrency bounds, live headroom and all source
ownership/checkpoint checks remain active.

Import status includes bounded runtime-only `performance` aggregates for scan,
read/parse, write/ACK, finalization and checkpoints. Concurrent per-file durations
overlap and cannot be summed into wall time. Writer diagnostics include
enqueue-to-batch-selection `queueWait` and `totalFlushDurationMs`; these do not
measure database WAL apply delay. Metrics reset on restart and contain no source
paths or payloads. Persisted job counters remain the import progress authority.

The [throughput acceptance](docs/archiver-import-throughput-acceptance.md) records
local comparison and limitations. Reproduce against disposable local QuestDB:

```sh
node --import ./node_modules/tsx/dist/loader.mjs scripts/compare-legacy-import-throughput.mts 4096 baseline /tmp/import-baseline.json
node --import ./node_modules/tsx/dist/loader.mjs scripts/compare-legacy-import-throughput.mts 4096 bounded-fast /tmp/import-fast.json
node --import ./node_modules/tsx/dist/loader.mjs scripts/accept-real-questdb-import.mts 2000 /tmp/import-faults.json import-hint
```

These runners create/drop only their own synthetic tables and temporary spools
on `127.0.0.1:9000`; they do not accept a production address. Normal live batching
and the existing stored-event replay path receive no new wait hint.

### Preparation during a legacy write

The importer prepares at most one following writer chunk while the current
chunk waits for its database acknowledgement. Both chunks belong to the same
bounded source pass: at most `min(batchSize, 2 * concurrency)` resident envelopes,
and their combined original file sizes stay within `maxBatchBytes`. This limits
source payload admission, not total Node RSS: decoded JSON, read buffers, the
writer queue and the SDK also consume memory. No whole-directory list is loaded.

Preparation does not accept an event into the writer. Pause, shutdown, lost live
headroom or database unavailability stop new writes; prepared files remain on
disk and are read again after reviewed resume. File identity and storage policy
are rechecked before writing, and source ownership is rechecked for each chunk
and after ACK before removing a file. Already accepted writes can finish during
pause/shutdown. Neither prefetch nor a successful HTTP response promises exactly
once delivery after a crash.

`performance.pipeline` reports bounded runtime aggregates: `readAheadChunks` and
maximum admitted file count/combined source size for the current and following
chunk. These are conservative admission bounds, not a heap measurement. Metrics
reset on restart and contain no file names, paths or contents.

See [pipeline acceptance](docs/archiver-import-pipeline-acceptance.md). On the
measured local disk, the gain over the previous fast importer was below 2%; the
pipeline should not be presented as another large throughput improvement.
Run slow-ACK, outage, pause and restart acceptance with actual read-ahead:

```sh
node --import ./node_modules/tsx/dist/loader.mjs scripts/accept-real-questdb-import.mts 2000 /tmp/import-pipeline-faults.json import-hint pipeline
```

### Sizing a legacy import

Default import passes now select up to **256 files**, with 64 writers, 16 MiB
combined source size and 100-ms spacing. Explicit `batchSize` and `intervalMs`
settings are preserved. Reading ahead still retains only the current and next
writer chunk. Larger passes reduce scheduling/checkpoint overhead; they do not
increase default live queue size or writer concurrency.

Import status includes `settings`, the effective startup limits. Requested
concurrency is capped at `max(1, floor(ingestQueueMaxEvents / 8))`; for example,
requesting 128 with the default 512-event live capacity still yields 64. These
limits reset only on restart, and changing startup config requires restart.

An **opt-in** measured profile uses 256 files/pass and 128 writers:

```json
{
  "archiver": {
    "ingestQueueMaxEvents": 1024,
    "legacyImport": { "batchSize": 256, "concurrency": 128, "intervalMs": 100 }
  }
}
```

The larger live event capacity allows the existing one-eighth concurrency cap;
it also raises the potential live event count in memory. The live payload byte
limit and legacy byte limit remain separate. Start with the default profile and
observe workload-specific memory/DB pressure before choosing this profile. A
small `questdb.batch.maxRows` still splits accepted work into smaller ILP batches:
merely increasing that threshold cannot create rows that have not been admitted.

The [batch sizing acceptance](docs/archiver-import-batch-sizing-acceptance.md)
compares the separate effects of pass size and actual ILP batch size. Local
20k-file results: about 30.6 seconds at 128/64, 21.0 seconds at 256/64 and
17.0 seconds at 256/128. These synthetic append-mode results do not forecast a
50M-file production import, mixed table packets or many-table/dedup workloads.

```sh
node --import ./node_modules/tsx/dist/loader.mjs scripts/compare-legacy-import-throughput.mts 20000 larger-pass /tmp/import-larger-pass.json
node --import ./node_modules/tsx/dist/loader.mjs scripts/compare-legacy-import-throughput.mts 20000 larger-both /tmp/import-larger-both.json reverse-days
node --import ./node_modules/tsx/dist/loader.mjs scripts/accept-real-questdb-import.mts 2000 /tmp/import-large-faults.json import-hint pipeline-large
```

### Shorter healthy import pacing (opt-in)

The default `archiver.legacyImport.intervalMs` remains **100 ms**. An explicit
value of **10 ms** is now accepted for a prepared backlog on a healthy database.
It only shortens the delay between successful bounded passes and the background
scheduler tick. The existing **5-second failure backoff**, file identity checks,
source ownership, checkpoint fsync and ACK-before-unlink remain in place.
Changing startup configuration requires a service restart.

For the measured 256-file/128-writer profile above, set `intervalMs` to `10`.
Keep monitoring live traffic, memory and database pressure; a shorter healthy
pause does not raise any queue, file or byte bound. The local 20k-file comparison
was 16.89 seconds at 100 ms and 9.09 seconds at 10 ms; the 100k-file short-pacing
run took 46.65 seconds. These are synthetic local fixtures, not a production
50-million-file capacity estimate.

```sh
pnpm exec tsx scripts/compare-legacy-import-throughput.mts 20000 short-pacing /tmp/import-short-pacing.json
pnpm exec tsx scripts/accept-real-questdb-import.mts 2000 /tmp/import-short-faults.json import-hint pipeline-large short-pacing
```
