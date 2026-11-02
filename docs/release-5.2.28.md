# Archiver 5.2.28 — selective final-table pause and durable target replay

The Archiver can pause its own writes to selected final QuestDB tables while
continuing MQTT intake and writing other destinations. Protected operator APIs
provide preview, status and revision-checked, idempotent pause/resume. A branch
preview lists observed final destinations and explains how new dataGroups are
covered. This is not a database-wide writer lock.

All eligible parsed packet parts now pass through a bounded SQLite FULL/WAL
journal before database writes, including when no selective pause is active.
During database failure, all destinations retain pending parts. On recovery,
unpaused tables replay automatically while selected tables remain held until
explicit resume. Equal consecutive MQTT payloads receive distinct occurrence IDs.
Startup while QuestDB is unavailable defers protocol negotiation without stopping
MQTT intake. Legacy import and older stored-event replay use the same final-table
gate; legacy originals remain until all packet parts have confirmed outcomes.

## Compatibility and operating changes

- Node.js 22.13 or newer is required; Runtime Node.js 24 is recommended.
- UNS Kit API/core stay on 3.0.41; no new SDK or Controller release is required.
- `event_storage/targets` is new persistent state. Retain its entire directory,
  including any SQLite WAL, along with the existing event storage. The portable
  operator policy lives in `archiver-target-policy.json` beside instance config.
- Healthy ingestion also incurs durable-journal IO. The startup-only limits
  default to a 1 GiB SQLite page budget, a conservative half-budget for payloads,
  64 MiB free-space reserve and one million queued parts per target. Validate
  sustained throughput, disk capacity and expected outage duration before rollout.
  Existing pre-parse overflow files are reported separately from this budget.
- Target storage failure is reported as failed acceptance rather than silently
  bypassing the journal bound. Finite storage cannot guarantee retention of
  input that its broker does not replay.
- Table names, units, packet times, source topics and ingestion modes remain the
  writer's existing rules. Existing global control remains separate; global
  resume does not clear selective rules.
- An operator role plus the normal JWT path grant is required. A passive instance
  permits status/preview but rejects target mutations. An unknown write result
  requires review and explicit confirmation before resuming.
- Existing generic Runtime and SDK application-state contracts carry pause
  policy. They do not copy the payload journal. Pending data blocks SDK policy
  export and must be drained or retained/reviewed separately. This release does
  not advertise a newly accepted two-controller live migration capability.

Before switching back to a version older than 5.2.28, review and clear selective
pauses and drain the target journal. Older versions do not honor this policy or
replay its SQLite payloads. Keep the state files until that review is complete.

A successful HTTP response or ILP acknowledgement does not prove WAL application,
replica visibility, or exclusion of external writers. Lost acknowledgements can
produce append-mode retry duplicates; this is not exactly-once delivery.

## Validation and rollout boundary

The local serial combined suite passed all 210 tests. The actual built Archiver
received MQTT, registered its JWKS-protected API, survived a local QuestDB
stop/start and an Archiver restart during that outage, and retained its pause.
All 14 test rows matched timestamps, values, units and topics, with an empty final
queue. A lost pause response, repeated resume, reader denial and passive-owner
protection were checked. The Controller's generic state file functions were also
checked for idempotent copy and target-conflict rejection without service-specific
Controller changes.

See [acceptance scope](target-control-acceptance.md) and [API/queue contract](../README.md#selective-target-control-5228).
Full Controller-managed installation, public proxy/UI acceptance and a real
cross-controller live migration remain separate checks before production use.
Publication does not deploy this version or pause production tables.
