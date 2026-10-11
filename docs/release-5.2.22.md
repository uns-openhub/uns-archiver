# Archiver 5.2.22 — recovery health and upgrade acceptance

Candidate patch: no storage layout, import limits, credentials, startup schema or
SDK dependency change. See [5.2.21 upgrade notes](release-5.2.21.md) for bounded
streaming import and acknowledgement/removal semantics.

Historical recovery now reports `running`, `paused`, `pending`, `blocked`, or
`completed` alongside existing dependency-health fields. Incomplete history remains
visible as degraded. Running/paused replay is distinguished from an unavailable
live MQTT writer. Errors, preserved/quarantined files and deferred work require
review. Completion clears the signal only when completion is confirmed and no
unfinished/error state remains. No source filenames, paths or payloads are added
to service metadata. Health publication remains periodic.

## Version replacement

Pause replay and wait for zero in-flight writes before handover. Retain the
original allowlisted source and its checkpoint. Copy only the reviewed node-local
source allowlist into the stopped target, not the source files. Wait for clean
old-process drain/exit and verify target live capture. Inspect the same paused job
and resume with the new owner's identity/current revision. A stale old-owner
request is rejected; no API force-unlock is provided. Autonomous SDK/signal stop
remains available without a controller. Preserve all old instance spools.

## Evidence and limits

A local controller 2.1.150, MQTT broker, PM2 and QuestDB drill upgraded published
5.2.21 to candidate 5.2.22. One 600-file sealed source paused after 24 writes at
revision 2; the target retained the same job/checkpoint and stayed paused. The old
process exited cleanly with no restart. Old-owner control returned 409; reviewed
resume reached revision 3 and completed all 600 files. 80 MQTT events across
handover and 20 after handover produced 700 distinct expected event IDs with exact
original timestamps. No quarantine/deferred files or failed flushes; final
controller health became healthy after publication/cache convergence. Source
files were not copied or moved. The importer version-replacement unit regression
also checks a live old claim blocks the target until clean close.

Full archiver verification: 174 tests, typecheck and build passed. This bounded
local case is not an exactly-once guarantee or a production 50M-file throughput
measurement. Controller 2.1.150 is needed for the new operator labels; older
controllers can consume the existing fields. No new GraphQL contract is required.
This acceptance record does not by itself imply publication or production rollout.
