# Shared operator API and input exclusion — local candidate

Base: published Archiver 5.2.29 (`5cb4f9aa`), Controller 2.2.63 (`febd6c67`)
and SDK 3.0.41 source (`f131c48d`). These changes extend the SDK and need a new
SDK release plus consumer dependency/lock updates before publication. They have
not changed production or granted a production workload any additional tools.
The coordinated 5.2.30 candidate now adopts published SDK 3.0.42 with a verified
registry lock and independent frozen installation; the original 3.0.41 source
description below records the prepublication development boundary.

The application declares existing status/preview/pause/resume using shared
version-one operation descriptors derived from its existing Zod schemas. The
Controller owns authenticated bounded orchestration and durable command review;
Archiver retains business validation, policy revision, receipts, write boundary
and journal ownership. There is no application-name branch in the generic host.

`target-control-operator.test.ts` uses actual SDK HTTP/PSS/JWKS validation and an
actual SQLite target journal. It verifies selective pause, idempotent reply-loss
retry, parent revocation/status outage, queued records from two tables during a
simulated database failure, journal reopen, explicit resume and four unique
replayed occurrences. `target-control-http.test.ts` retains native operator/admin,
reader, anonymous, forged-token, wrong-path and passive-owner coverage.

`archive-topic-admission.test.ts` verifies valid/invalid MQTT exclusion filters,
subtree/exact/one-level boundaries, reload in both directions, no admission of new
excluded occurrences, and preservation/replay of events accepted before a policy
change. Existing configurations remain valid. Filters run only at MQTT admission;
accepted journal/file replay and legacy imports are never filtered or deleted.

The optional actual-process acceptance starts the built Archiver against isolated
local MQTT/QuestDB, tests target pause/outage/restart and exact values/timestamps/
units/topics, then updates the configuration file and waits for normal live
refresh. The excluded input is counted and not written, another topic continues,
clearing the exclusion restores future input after restart, and cooperative Stop
exits naturally. A standalone discovery/JWKS fixture is used; this is not a claim
of a normal Controller-managed installation or a released-artifact rehearsal.

See the private Controller's companion acceptance for actual MCP/GraphQL/SQL
concurrency and Controller-restart/lost-response evidence. Database acknowledgement
is distinct from replica visibility; no database-wide writer exclusion or
exactly-once remote-effect guarantee is added. A configured exclusion permanently
skips new occurrences until removed; it is not a maintenance pause.

The final default suite passes 226 tests, with two explicitly optional fixture
checks skipped. The Controller's additional MCP acceptance loads this built target
API/journal directly and verifies draining/paused distinction, concurrent revisions,
same-ID reply-loss recovery, actor/body conflicts, explicit uncertain-write
reconciliation and preservation of 409/503 error details. These are local component
checks, not production table operations or a normally installed cluster rehearsal.

Two capacity tests verify bounded configuration limits and reopening the same
journal with a larger budget without losing pending parts, policy or receipts.
The reopen fixture uses zero free-disk reserve only because its local disk is small;
no production-sized ten-million-part test is claimed. Status adds
`payloadBudgetBytes`, `maxPendingPerTarget` and `reserveBytes` so an operator can
inspect effective limits. These fields are not yet in the published 5.2.29 build.
