# Archiver 5.2.23 — first-event registration and buffered ingest

Newly registered attributes can receive events before the controller's active
registry catches up. Eligible buffered events now enter the existing bounded
live ingest queue and share QuestDB batches. Registry refresh no longer waits
for a separate flush acknowledgement for every buffered event. Existing durable
overflow and replay preserve acknowledged write/removal semantics.

Unknown-topic observations request a coalesced refresh at most every five seconds,
without overlap. The ordinary 30-second poll remains. Unknown packets retain
existing count/age limits; broad MQTT subscriptions do not grant archive eligibility.
Shutdown cancels the expedited timer and rejects late registry admission.

No new configuration, GraphQL contract, storage layout, deduplication key or SDK
dependency is required. Preserve all instance spools, import checkpoints and sealed
legacy sources during normal version handover; see [5.2.22 upgrade notes](release-5.2.22.md).

## Verification

- Local full verification: 179 tests, typecheck and build.
- Real MQTT → controller registry → archiver → QuestDB event-ID accounting:
  normal-speed new-path run and fast first batch after demo restart each retain
  all 10 transitions; five complete visits and inspection open in the normal UI.
- Startup overflow, shared bounded queue admission, unaccepted-tail retention,
  inactive-topic exclusion and refresh coalescing/rate limit/cancellation covered.
- [Acceptance and limits](cold-start-metadata-buffer-acceptance.md).

This is not an exactly-once or immediate-visibility guarantee. Packets observed
before subscription or beyond the bounded inactive-topic grace period are outside
this guarantee. A control run confirms large visibility delays, not permanent
loss in every earlier report. No production operation is part of this release.
