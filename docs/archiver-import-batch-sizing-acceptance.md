# P4e: legacy import batch sizing

Date: 2026-10-09. Candidate archiver 5.2.20 / Kit 3.0.23, base 79d6dba.

## Acceptance contract

Measure event/pass size and writer concurrency separately against real local
QuestDB, preserving shared sender serialization, ACK-before-unlink, byte bounds
and the existing runtime concurrency cap (one eighth of live event capacity).
128 writers require at least 1024 live queue capacity; do not silently relax that
cap or change the default live queue. Retain one controller plus existing infra.

Compare 128/64, 128/128, 256/64 and 256/128. Verify actual ILP batch size, distinct
rows and original timestamps, status/event loop and RSS. Test a multi-day
historical timestamp fixture, slow ACK, 503, pause/restart/resume and live traffic
coexistence. Change a default only on measured benefit, preserve explicit config.
No production operation, release or physical 50M-file capacity claim.

## Decisions and implementation

Default source pass size increases 128→256, preserving the 64-writer default,
16 MiB source-byte cap, one-chunk read-ahead, 100-ms pacing and live queue default.
Explicit startup pass size/pacing remains authoritative. Metadata selection
remains bounded by 512 entries and the cooperative 25-ms scanner budget. More
work per pass can delay other source IDs sharing this manager; pause/headroom
checks still run before preparation and each writer admission. This is not a
hard per-pass latency guarantee on a slow filesystem.

128-writer tuning is opt-in: the existing one-eighth event-capacity cap requires
1024 live event capacity. The cap is extracted into a tested helper with identical
semantics, not relaxed. Import API now reports a copied `settings` object with
effective startup limits so a configured 128 that is capped to 64 is observable.
Byte caps remain unchanged; increasing event capacity can still increase memory
when many small live packets arrive. Normal writer defaults are unchanged.
Config schema/types are regenerated; there is no new config field or CLI dependency.

## Measurements

All benchmark fixtures have one numeric data row per file, one random WAL table
with designated `time`, append mode and no identity enrichment. The writer uses
512 rows / 2048 pending / 1000 ms and a 25-ms legacy hint. Fixture creation and SQL
verification are outside import timing. Sequential runs share host caches and
background infra, so results are local observations rather than production capacity.

| Files | Files/pass | Writers | Live event capacity | Import seconds | Actual max ILP rows | Successful flushes |
| --- | --- | --- | --- | --- | --- | --- |
| 4,096 | 128 | 64 | 512 | 6.289 | 64 | 64 |
| 4,096 | 128 | 128 | 1024 | 5.329 | 128 | 32 |
| 4,096 | 256 | 64 | 512 | 4.296 | 64 | 64 |
| 4,096 | 256 | 128 | 1024 | 3.354 | 128 | 32 |
| 20,000 | 128 | 64 | 512 | 30.568 | 64 | 313 |
| 20,000 | 256 | 64 | 512 | 20.975 | 64 | 313 |
| 20,000 | 256 | 128 | 1024 | 16.974 | 128 | 157 |

The default pass increase measured **1.46×** the previous fast importer on 20k;
the opt-in combined profile **1.80×**. Their gains have different causes: the larger
pass reduces pacing/checkpoint overhead, whereas 128 writers fill larger actual
ILP batches and halve flush count. Raising only the threshold would not do that.
Every run independently verified all unique values and original timestamps.

A separate 20k opt-in run assigned timestamps in inverse fixture-ID order at one
minute intervals, spanning almost 14 days and multiple partitions. Filesystem
iteration order is not guaranteed descending time; no global sorting is claimed.
It took 16.570 s / 157 flushes / max 128 rows, with exact values/times and an
independent SQL latest-value/time check. Historical times were not replaced by now.
Maximum sampled harness RSS was 180.6 MiB, event-loop delay 14.02 ms and cached
status helper max 0.590 ms. Many-table, table-packet and dedup/window-replace costs
are outside these throughput fixtures; existing regression contracts remain.

## Recovery checks completed

- **168 tests**, typecheck and production build pass. Three new tests cover the
  unchanged runtime concurrency budget, copied effective defaults, and explicit
  batch size/pacing overrides. Strict runner types, formatting and diff check pass.
- Real slow ACK with larger profile: 256 prepared / 128 accepted / zero acknowledged
  before ACK. Pause took 8.43 ms, accepted 128 drained, read-ahead stayed on disk.
- Real 503 outage: 35 failed requests, 256 deferred files, no new ACK counter.
  Restart stayed paused until explicit resume; all 2,000 values/timestamps then
  verified without duplicates or remaining source events.
- Full RTT runtime: normal admin authentication and public Caddy HTTP 200, 2,000
  import files plus 100 concurrent live MQTT events. Live rows were visible during
  import; direct status max 12.95 ms. Exact accumulated oracle verified 7,406 IDs
  and times. Effective status reports 256 files / 128 writers. Max observed batch
  132 includes co-batched live rows: writer is configured at 512 rows / 2048 pending
  / 100 ms here, not the benchmark's 1000-ms interval or the P4d runtime's 32-row
  threshold. Full import/live/verification took 2.18 s and is not a pure import timing.

- Full runtime shutdown with controller absent: hold the first actual QuestDB ACK,
  observe 256 prepared / 128 accepted / zero acknowledged across 512 source files,
  stop the controller and send SIGTERM. The archiver waits for accepted ACKs,
  exits 0, PM2 stopped/restarts 0, and persists paused/shutdown-review-required with
  exactly 128 acknowledged and **384 retained files**. About 249-ms exit detection
  includes a deliberate 100-ms hold and polling; it is not a shutdown SLA.
- Restore the one controller and the same reviewed opt-in local config. The import
  remains paused; explicit normal API resume completes it. Independent SQL oracle
  verifies **7,918 unique IDs and original times**, no gaps/duplicates/mismatches;
  all six jobs completed, old 5.2.17 source remains stopped. The fault proxy is
  removed. Local archiver retains the explicit 1024-event/128-writer test profile.

No new browser interaction was exercised; P4b's browser evidence is separate.
No production operation, commit, tag or publication. The source-byte limit is not
a total Node RSS bound, and ACK does not guarantee exactly-once delivery through a
crash before unlink/checkpoint. No filesystem checks or fsync have been removed.

## Next boundary

P4 throughput/control checks close this local slice. Return to P2's real
caller→HTTP→SQL slow-query attribution and load-shedding/coalescing acceptance,
then production filesystem/retained-volume/cutover preflight. A real 50M-file
filesystem and production QuestDB capacity remain unmeasured; a native helper is
not justified by the current evidence of pacing/batch overhead.

## 100k real-file scale check

Opt-in 256/128 profile: first run 85.922 s, repeated run **85.886 s**,
about **1,164 files/s**, 782 successful flushes, max 128 rows. Both runs verify all
100,000 unique values and original timestamps, with zero remaining source events.
The repeated run also independently verifies SQL latest value/time. No throughput
forecast for 50M files follows from these tests.

The first aggregate RSS sample (258.0 MiB) mixed import and SQL-oracle work. The
runner now records phases separately. Repeated **sampled whole-process RSS during
import was 236.4 MiB**, and combined harness/verification sample 253.0 MiB.
This is higher than the 20k samples; constant total RSS is not claimed. Admission
still retained at most 256 current/next envelopes and the original byte cap.
Fixture generation and the loaded SDK remain part of the process baseline, while
the all-row verification allocates its own result array/Set. Logical admission
bounds and these measurements do not prove a production memory ceiling.

At 100k cached status helper maximum was 0.624 ms and event-loop maximum 28.61 ms
(includes verification). Temp files and this run's random table were cleaned up.

[Machine-readable evidence](evidence/archiver-import-batch-sizing-2026-10-09.json).
