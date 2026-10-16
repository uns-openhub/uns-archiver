# Cold-start metadata buffer — local acceptance

Status: locally verified; candidate release 5.2.23. Publication is tracked by its
GitHub PR, tag and release checks; no production deployment is included.
Base: Archiver v5.2.22. No configuration fields, SQL schema or deduplication keys
changed.

## Problem and change

A new material can publish transitions before the controller registers its
attributes. The inactive-topic buffer correctly delays eligibility, but its old
flush loop awaited each QuestDB acknowledgement serially. A snapshot with many
events could block further registry refreshes long enough for newer unknown
packets to exceed the metadata grace period or count limit.

Eligible entries now transfer to the existing bounded live ingest queue. They
share ILP batches; capacity overflow uses the existing durable spool. Entries for
unregistered paths remain in the bounded metadata buffer. Failed synchronous
handoff retains the unaccepted tail. Unknown-topic observations request one
coalesced refresh, at most every five seconds and never overlapping; normal
periodic refresh remains. Shutdown cancels pending expedited refreshes. Registry replies received after
shutdown starts cannot re-admit buffered events into the drained writer.

## Local verification

- 179 tests pass. Typecheck and build pass.
- Real MQTT, Controller 2.2.6, API 4.1.17, Demo 6.1.19 and QuestDB, with scoped
  read credentials. Existing local database; two newly registered material IDs.
- Normal-speed new-path run and fast first batch immediately after demo restart:
  **10 captured transitions / 10 matching stored event IDs** in each run.
- All five recorded visits and inspection load through the normal UI.
- The fast first batch exercises 1 ms adjacent transitions and startup durable
  overflow. An early database read precedes the final replay acknowledgement;
  the eventual exact ID read-back passes without restarting the archiver.
- Queue limits remain 512 events / 16 MiB; no processing failures observed.
- Unit regressions cover shared admission before individual ACK, retained tail
  after handoff failure, bounded overflow, exclusion of inactive paths, refresh
  coalescing, no overlap, rate limiting and cancellation.

A control run eventually wrote every event too, while showing serial flush and
registry delays. It proves a timing defect, not permanent loss in every earlier
report. This fix does not guarantee storage before subscription, through a
prolonged registry outage, or after the intentionally bounded inactive buffer
expires. Unknown telemetry is not silently made durable. Production was neither
accessed nor changed.
