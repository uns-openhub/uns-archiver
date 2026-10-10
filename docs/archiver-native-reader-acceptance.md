# Optional native reader and healthy import pacing

> **Historical experiment — abandoned on 10 October 2026.** The uncommitted
> Go reader and build scripts were removed at the user’s request. There is no
> native reader in the runtime or release candidate. The measurements below
> document the experiment; only the Node healthy-pass pacing change remains.

Acceptance contract, 10 October 2026. Preserve the existing source claim,
bounded passes/bytes, file identity checks, original times, ACK-before-unlink,
checkpoint fsync, failure backoff and autonomous drain. Node remains the default.

First compare a read-only Go prototype with Node on the same small files and
bounded 128-file chunks, including IPC and JSON parsing. The binary must not
write, rename, delete, claim sources or connect to QuestDB. No automatic binary
download or Go requirement for normal Node installation/build/start.

Only integrate a runtime reader if end-to-end benefit is measured. Separately
compare shorter explicit healthy-pass pacing with the existing 100-ms profile
against real local QuestDB. Keep failure delay and explicit old settings intact;
test pause, slow ACK, 503, restart/resume and original row/time results.

The previous 100k run spent about 41.5 seconds between passes and 8.7 seconds in
checkpoint operations during an 85.9-second import. Per-file read/ACK/finalize
timings overlap, so their totals cannot be added as wall time or used to claim
the largest CPU cost. Native reading may be faster in isolation yet offer little
end-to-end benefit. These are local fixtures, not physical 50M production proof.

Status: local implementation and acceptance completed below. No release or
production operation is part of this slice.

## Results

P4f completed in the local scope below. See the
[sanitized evidence](evidence/archiver-native-reader-2026-10-10.json).

| Measurement                                                 | Result                                             |
| ----------------------------------------------------------- | -------------------------------------------------- |
| Same-host 20k, 256 files/pass, 128 writers, 100-ms interval | 16.887 s, 1,184 files/s                            |
| Same-host 20k, same limits, 10-ms interval                  | 9.088 s, 2,201 files/s                             |
| 100k, 10-ms interval                                        | 46.647 s, 2,144 files/s                            |
| 20k, 10-ms interval, reverse historical day order           | 8.635 s, original times/latest verified            |
| Full authenticated local Runtime                            | 2,000 import files + 100 live MQTT events, 2.324 s |

The paired 20k test reduced elapsed time by **46.2%** with exactly the same
157 successful ILP flushes (at most 128 rows). Measured between-pass idle time
fell from 8.271 s to 0.825 s. There were zero missing values, duplicates or time
mismatches in independent SQL checks. The 100k run used 782 flushes and retained
at most 256 source files/39,300 admitted source bytes in the two chunks. Sampled
Node import RSS reached about 267 MiB and event-loop maximum delay 32.46 ms;
these are process measurements, not a claim about production memory or 50M.

### Optional Go reader

Two 20k-file comparisons used Node/Go/Go/Node order on the same files. The initial
Go executable was x86_64 on the arm64 host (0.329/0.330/0.323/0.296 s), so that
exploratory comparison is not an architecture-matched ranking. A rebuilt
native-arm64 comparison was about
0.698/0.323/0.320/0.290 s. The first Node pass is sensitive to filesystem cache
and concurrent local activity. Warm runs do **not** show a consistent Go gain.
IPC/base64 and Node packet parsing are included; combined Node+Go RSS and full
import performance are not measured. Do not attribute a cold-pass advantage to
Go or present this as a faster production importer.

The Go binary remains an explicitly built read-only **standalone prototype**.
It is not started by the runtime, selected in service configuration, downloaded
or required by normal Node build/verify/start. No production backend switch was
added. Revisit integration only after a representative filesystem experiment
shows an end-to-end benefit. Go 1.25 stdlib only; optional build supports Linux
and macOS amd64/arm64. Linux amd64/arm64 cross-builds passed; execution tests were
on macOS arm64 only.

Bounds: 128 selected files/request, 1 MiB/file, 16 MiB combined source bytes,
128 KiB request metadata and bounded response IPC. Pinned directory/device/inode,
regular-file/link/inode/size/exact nanosecond mtime checks reject changed or
unsafe selections. No writes, claims, rename, unlink or DB access. The comparison
wrapper has a 5-second request/shutdown deadline, kills a failed helper and never
silently substitutes another backend. Five Go tests cover identities, symlinks,
hardlinks, traversal, byte/count bounds, size/time changes, FIFO and empty files;
race detection passed. A hanging helper was rejected and terminated.

### Failure and shutdown acceptance at 10 ms

- Real QuestDB, 2k files: held ACK permits one bounded read-ahead; pause remained
  responsive (8.43 ms). Injected 503s preserved unacknowledged source files.
  Restart remained paused; explicit resume finished all 2k with zero duplicate
  rows/time mismatches and no residual event files. Existing 5-second failed-pass
  backoff remains in force; QuestDB SDK retries are separate from that backoff.
- Authenticated normal Runtime/proxy: 2k import + 100 QoS1 live MQTT events;
  live rows were visible while importing. SQL verified all 12,118 accumulated
  event IDs and original times, public proxy200, maximum status HTTP1.93 ms.
- Controller absent, held ACK: SIGTERM waited for 128 accepted writes, discarded
  no unacknowledged source, exited0 without restart; checkpoint paused for review
  and 384 of 512 events remained. After controller restoration no automatic
  resume occurred; explicit resume yielded all 12,630 accumulated unique IDs and
  original times, nine completed source jobs, public proxy200. Old5.2.17 stayed
  stopped and fenced; API Global remained independent and online.
- The first full Runtime oracle hit its own 10k-row query cap after the import
  succeeded. A separate new source/live range was then tested with an explicit
  13k SQL cap; no test fixture was replayed to manufacture a success.

**Separate observed rollout gap:** the local controller closed HTTP on SIGINT
but did not exit after SIGTERM either. The local test restoration required
forced termination of that exact controller process. This is not an archiver
shutdown failure and is not production approval. Awaited shutdown stages and
open sockets/MQTT must be diagnosed and the fix acceptance-tested before rollout;
the root cause has not been proven.

Node: 170 tests passed, strict script typecheck and normal build passed;
`git diff --check` passed. Defaults remain 100 ms and all limits/ACK/fsync/source
checks are preserved. Local Runtime now uses the explicit 10-ms profile, one
controller, one API Global and one current archiver. No new commit, release,
published binary, production operation or browser workflow acceptance.

## Follow-up decision, 10 October 2026

The user abandoned the Go experiment. Its uncommitted source, tests, comparison
runner and optional build script have been removed from the candidate. Historical
measurements above remain evidence only; there is no Go feature to build or deploy.
Shorter healthy import pacing and the Node implementation remain.
