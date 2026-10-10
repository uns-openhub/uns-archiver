# P4c: bounded legacy import throughput

Date: 2026-10-09. Candidate archiver 5.2.20 based on `79d6dba`, published Kit 3.0.23.
Existing P4b changes in this isolated checkout are retained. No production access,
release bump, tag, publication or unrelated controller source change.

## Acceptance contract

Use the same synthetic event records and real local QuestDB HTTP ILP. Compare
legacy 128-file/64-writer/500-ms settings against a bounded 25-ms per-row flush
hint and 100-ms import pacing, keeping normal writer settings at 512 rows /
1000 ms / 2048 pending rows. Fixture creation and SQL verification are outside
import wall time. Every run creates its own random WAL table and temporary spool,
then removes only those fixtures. One controller and existing infra stay running.

Required: independent exact ID/value and historical timestamp oracle, no source
file removal before ACK, bounded memory/status work, pause, unavailable database,
reviewed resume, autonomous drain, live traffic coexistence and no parallel use of
the shared mutable ILP sender. Do not extrapolate this local run to 50M real files.

## Measured results

| Real file fixture | Profile | Import wall time | Files/s | Independently verified |
| --- | --- | --- | --- | --- |
| 4,096 | Original code/settings | 82.31 s | 49.8 | 4,096 |
| 4,096 | Same reference settings, phase diagnostics added | 82.21 s | 49.8 | 4,096 |
| 4,096 | 25-ms hint / 100-ms import pacing | 6.46 s | 634.3 | 4,096 |
| 4,096 | Repeated fast profile | 6.33 s | 647.4 | 4,096 |
| 20,000 | Fast profile | 31.30 s | 639.1 | 20,000 |

Repeated instrumented comparison observed **13.0× faster** import. All
runs verified unique fixture values and original timestamps, with zero duplicate
rows and no remaining source events. Fixture generation, SQL verification and
cleanup are outside these wall times. Sequential local runs share host caches
and background services; this is reproducible local evidence, not a rigorous
production capacity model or an extrapolated 50M-file estimate.

Reference writer queue waiting averaged
**999.3 ms/row**;
repeated fast average was
**24.6 ms/row**.
Both used 64-row actual batches despite a 512-row threshold. The 20k run made
314 successful HTTP ILP batches, peak sampled
harness RSS 177.2 MiB, maximum event-loop
delay 17.84 ms, and maximum synchronous cached-status
helper duration 0.272 ms. Harness RSS includes verification;
it is not the full application RSS or a 50M-file memory measurement.

## Recovery and actual runtime checks

- Real QuestDB fault runner with hint: 2,000 rows verified; pause answered in
  9.58 ms while slow writes drained.
  A 503 outage produced 21 failed HTTP attempts, retained 64 deferred files and
  did not increase acknowledged files. Restart required explicit resume.
- Full candidate runtime installed into the isolated RTT fixture: authenticated
  import of 2,000 sealed-source files, 100 concurrent live MQTT events. Live
  rows were queryable during import. Public Caddy import-status HTTP 200;
  direct runtime status maximum 3.70 ms.
  Exact SQL oracle verified all 2,900 IDs and original times including prior P4b.
- Controller absent + candidate SIGTERM: 18 queued rows drained, exit 0,
  PM2 stopped, restart count 0. Approximately 102 ms exit detection uses 100-ms
  polling and is not a hard shutdown SLA. The import was already completed;
  this proves live drain after optimization, not full-application shutdown with
  a legacy batch in flight (module regression covers that case separately).
- Restored one controller: both completed jobs persisted and exact SQL oracle
  verified all 2,950 unique IDs/timestamps, zero gaps/duplicates/mismatches.
- **157 tests**, typecheck, production build and standalone script strict types
  passed; config schema/types regenerated. Source checks and fsync unchanged.

The normal stored-event replay path and live-only batching receive no hint.
A legacy hint can also flush co-batched live rows sooner, so transaction frequency
may increase while import runs. Explicit longer legacy pacing is preserved.
Only legacy default pacing changes to 100 ms. Per-file metrics still show IO
finalization overhead; no claim is made that Node CPU is the bottleneck.

Machine-readable [evidence](evidence/archiver-import-throughput-2026-10-09.json).

## Timing interpretation

`performance` in import status is bounded in-memory aggregate state for the
current runtime. It resets on restart; durable job counters remain authoritative.
`scan` and `checkpoint` record operation wall time. `readParse`, `writeAck` and
`finalize` accumulate per-file durations, including asynchronous waits: concurrent
values overlap and **must not be added to calculate total import wall time**.
`writeAck` includes packet validation and writer waiting, not just HTTP latency.
`betweenPassIdleMs` is wall time between finished and next started passes, including
pause/headroom/scheduling delay. It is not exclusively timer sleep.
Writer `queueWait` measures enqueue-to-batch-selection, excluding table setup,
row construction and network ACK; total flush duration includes failed/retried
flush operations. None of these counters contain payloads, names or source paths.

## Implementation scope

Only legacy writes pass the 25-ms hint. It can shorten the shared batch deadline
but cannot postpone an earlier deadline or resolve writes before a real ACK. Live
writer config and bounded queue capacities remain unchanged. The scanner retains
its 512-entry/25-ms cooperative limit. Default legacy pacing changes 500→100 ms;
an explicitly configured interval is preserved. Source ownership/file identity
checks, checkpoint fsync, live headroom, error backoff and shutdown drain remain.
[P4d](archiver-import-pipeline-acceptance.md) subsequently adds bounded preparation
of one following writer chunk. Ownership-check IO reduction, bounded timestamp
sorting, segmented future spools or a native helper remain separate measured work.
