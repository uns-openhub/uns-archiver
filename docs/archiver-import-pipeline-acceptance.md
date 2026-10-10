# P4d: bounded legacy-import preparation pipeline

Date: 2026-10-09. Candidate archiver 5.2.20, Kit 3.0.23.

## Acceptance contract

Prepare at most one following writer chunk while the current chunk awaits its
QuestDB ACK. Retain the existing source batch event/byte limits, serialized ILP
sender and on-disk source until acknowledgement. No full-directory inventory,
new configuration setting, production operation or release.

Prove preparation overlaps a pending write, bounded residency, pause/headroom/
shutdown preventing new writes, file and policy changes preserving the source,
restart review and exact timestamp/value verification against real local QuestDB.
Compare with a saved P4c source snapshot using identical fixtures/settings.
Record actual gains or regressions; no 50M-file throughput extrapolation.

## Implementation

Read/parse and write/ACK/finalize are separate stages. One following chunk may
be prepared while accepted writes wait; no next source pass is scanned ahead.
The same event/byte caps cover both chunks, so increasing total directory size
does not increase the in-memory filename list. Raw source size is capped; JSON
objects, temporary buffers and SDK state are additional bounded work, not a
claim that process RSS equals the source byte cap.

Admission now explicitly checks the persisted job state as well as shutdown,
command work, live headroom and database availability. A completed pause command
cannot accidentally allow the next chunk merely because command handling ended.
Prepared file identity is checked again before writing; policy and source owner
are checked before each chunk. The post-ACK source/owner checks, fsync and error
backoff are preserved. The shared ILP sender remains serialized.

Preparation failures become bounded result records; both preparation and accepted
writes are drained before closing/checkpointing. Discarded preparations leave
source files intact. No added startup setting or source-format change.

`performance.pipeline` exposes only runtime aggregate admission bounds. The
maximum covers current and next selected chunk conservatively, including a next
chunk that may subsequently be skipped; it is not exact heap residency.

## Local comparison

All runs use real files and local QuestDB HTTP ILP, a designated `time` column,
128 files/pass, 64 writers, 100-ms pacing and a 25-ms legacy wait hint. Writer
settings are 512 rows / 2048 pending / 1000 ms. A saved P4c source snapshot was
run first, then the candidate; fixture generation and SQL verification are
outside import timing. Independent SQL verifies unique values and original times.

| Files | P4c reference | P4d pipeline | Candidate files/s | Exact rows/times |
| --- | --- | --- | --- | --- |
| 4,096 | 6.364 s | 6.265 s | 653.8 | 4,096 / PASS |
| 20,000 | 30.755 s | 30.516 s | 655.4 | 20,000 / PASS |

Observed differences are **1.6% and 0.8%**, within plausible local-run variation.
This does not establish a material throughput gain. Actual batches still contain
64 rows. The earlier P4c reduction in waiting remains the major measured gain.
At 20k the pipeline admitted at most 128 envelopes / 19,660 original file bytes;
maximum sampled harness RSS was 179.9 MiB, event-loop delay 15.07 ms, cached status
helper maximum 0.258 ms. Those fixture sizes and RSS are not a 50M-file estimate.

## Recovery acceptance

- **165 tests** pass, including eight new cases: overlap/bounds, pause, shutdown
  and reviewed resume, live headroom, changed prepared files, policy change,
  large-file byte cap, lost owner. Typecheck, production build, strict runner
  types and diff check pass.
- Real delayed QuestDB ACK: 128 files were prepared while only 64 writes were
  accepted; zero source files acknowledged before ACK. Pause answered in 8.15 ms
  and only the accepted 64 files drained. The following chunk stayed on disk.
- Real 503 outage: 35 failed HTTP attempts, 128 deferred files and unchanged ACK
  counters. Restart stayed paused until explicit resume; all 2,000 fixture values
  and times subsequently verified, no remaining source events.
- Full local RTT runtime: normal administrator authentication, public Caddy HTTP
  200, 2,000 imported files plus 100 concurrent live MQTT events. Live rows were
  queryable during import. Direct status maximum 1.96 ms. SQL verified all 5,050
  accumulated fixture IDs/times without duplicates. Runtime writer here uses
  32 rows / 256 pending / 100 ms, not the comparison's default writer settings.
- Full runtime with controller absent: hold the first actual ILP ACK, observe
  128 prepared / 64 accepted / zero acknowledged, stop the controller, then send
  SIGTERM. The archiver stays alive until the held ACK is released. It exits 0,
  PM2 stopped/restarts 0, records paused/shutdown-review-required, acknowledges
  exactly 64 and retains the other 192 of 256 files. Exit detection took about
  252 ms including a deliberate 100-ms hold and polling; this is not a hard SLA.
- Restored original local QuestDB config and one controller. Import remains paused
  after restoration. Explicit API resume completes it; exact SQL oracle verifies
  **5,306 unique IDs and original timestamps**, zero gaps/duplicates/mismatches.
  All four import jobs completed; the old 5.2.17 source process remains stopped.

No new browser workflow was exercised in P4d; the prior P4b UI acceptance is
separate. No production operation, release, commit, tag or publication. Candidate
dist and installed RTT importer hashes match in the evidence.

[Machine-readable evidence](evidence/archiver-import-pipeline-2026-10-09.json).

## Remaining work

Measure larger actual ILP batches before changing defaults, including QuestDB
backpressure and historical timestamp/OOO cost. Then complete P2's real
caller-to-HTTP-to-SQL acceptance and production filesystem/volume/cutover preflight.
File/owner checks and fsync have not been removed to improve benchmark numbers.
Node CPU has not been established as the bottleneck; no native helper is added.

Subsequent [P4e acceptance](archiver-import-batch-sizing-acceptance.md) measures
larger passes and actual ILP batches separately. Its 256-file default and optional
128-writer profile supersede P4d's default sizing; P4d's recorded measurements
and source hashes remain historical evidence.
