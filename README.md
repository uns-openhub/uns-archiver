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
- Exposes authenticated control endpoints for status, pause, resume, and topics.

## Requirements

- Node.js 22 or newer
- pnpm 10
- A running UNS OpenHub controller and MQTT broker
- QuestDB with HTTP line protocol enabled

## Configuration profiles

The service ships three topology-specific profiles. They contain no deployment
credentials or customer endpoints.

| Profile | Use it when | MQTT and QuestDB | Service credential |
| --- | --- | --- | --- |
| `config-development-host.json` | Running the service directly with `pnpm run dev` on the host | `localhost` | `UNS_SERVICE_TOKEN` from untracked `.env` |
| `config-development-podman.json` | Deploying through a local Podman OpenHub controller | Compose DNS: `mosquitto`, `questdb` | Controller-managed `UNS_SERVICE_TOKEN_FILE` |
| `config-production.json` | Creating a production controller instance | Compose/Runtime DNS: `mosquitto`, `questdb` | Controller-managed `/run` token file |

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

The control API must use either `uns.jwksWellKnownUrl` or `UNS_API_JWT_SECRET`.
JWKS is preferred when the archiver runs alongside UNS OpenHub.

### Ingest backpressure

`archiver.ingestQueueMaxEvents` (default `256`) and
`archiver.ingestQueueMaxBytes` (default `16777216`, 16 MiB) bound live MQTT
payloads while QuestDB is slow. Excess messages are synchronously persisted to
`./event_storage` and replayed after live ingest is clear; they are not kept in
an unbounded in-memory promise backlog. `archiver.ingestConcurrency` defaults
to `1`; only raise it after measuring QuestDB and process memory under load.

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
