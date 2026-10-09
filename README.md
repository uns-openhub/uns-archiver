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
one. This is not yet a legacy-directory import API or a handover drain hook.

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
