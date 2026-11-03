# Archiver 5.2.30 — operator declarations, input exclusions and capacity visibility

The candidate consumes UNS Kit 3.0.42 and declares its existing selective-target
status, preview, pause and resume API through the reusable operator contract.
Controller 2.2.64 can therefore discover and review these operations through the
generic MCP/GraphQL host. Business validation, revisions, SQLite receipts and the
write boundary remain Archiver-owned. Declarations do not grant platform access.
Normal native operator API access and archiving retain their existing behavior.

`questdb.excludeTopics` optionally excludes new MQTT occurrences by standard MQTT
filter, for example `example/maintenance/#`. The normal live configuration refresh
can change the filter. Exclusions never delete or skip already accepted journal
parts, stored events or legacy replay, and do not remove existing database rows.
They permanently omit future matching occurrences while enabled; use a selective
target pause when accepted data must be retained during maintenance.

Target status additionally reports effective `payloadBudgetBytes`,
`maxPendingPerTarget` and `reserveBytes`. Existing bounds/defaults are unchanged;
capacity settings still require restarting the same instance on the same retained
journal. This release does not automatically enlarge production limits.

See [local acceptance](operator-api-acceptance.md) and the
[selective target contract](../README.md#selective-target-control-5228).
The journal location/format, configuration profiles, topics and units remain
compatible with 5.2.29. No backup-helper state contract change is required. A
successful pause receipt still requires observing a settled boundary; an unknown
database write requires explicit reconciliation before resume. External writers
and replica/WAL application are outside the Archiver boundary.

Registry 3.0.42 integrity and frozen adoption are verified. The complete local
serial `verify` passed 226 tests (two optional fixture checks skipped), typecheck
and build. The post-install typecheck/build and exact v5.2.30 tag check passed.
Controller MCP tests use the actual built target API and SQLite journal, including
reply loss, concurrent revisions, draining/paused and explicit uncertain-write
reconciliation. Source publication is being prepared; this document does not claim
installed-cluster acceptance or a production maintenance operation.

For later deployment, first update the participating Controller hosts to 2.2.64,
then perform the application's usual guarded upgrade and explicitly review the
new operator-tool grants. Retain the same journal and configuration. Active pause
policy or an unknown write/command remains a blocker to migration; do not remove
state or invent a new request ID to bypass it.
