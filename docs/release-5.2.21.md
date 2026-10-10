# Archiver 5.2.21 upgrade notes

## Live capture and legacy backlog are separate

Keep the old instance and spool mount. Use reviewed handover when supported,
otherwise a reviewed cold boundary. Confirm the old process exited and cannot
auto-restart before letting the new importer own its backlog. The new live
instance writes to its own storage; old `.event` and supported `.processing`
files are imported later from an operator-allowlisted node-local source.

Configure sources in instance-local `legacy-sources.json` or the startup config,
then inspect/confirm/start through the authenticated legacy import API. Source
paths cannot be supplied in arbitrary HTTP commands. See the README for the
API, instance binding, `expectedRevision`, `requestId`, pause/resume and source
closure review. A crash can repeat an acknowledged write: no exactly-once claim.

## Bounds and tuning

- Streaming directory inspection never loads/sorts/counts the whole directory.
  Status stays cached and labels unknown/lower-bound counts honestly.
- Default passes admit up to 256 files, 64 writers, 16 MiB source bytes and
  100 ms spacing. One following writer chunk may be prepared while awaiting
  the current ACK; pause/lost ownership/DB failure stop new admission.
- Legacy partial batches request at most 25 ms queue wait. Live batching keeps
  its existing settings. Files are removed only after actual writer ACK and
  source/file/policy revalidation. Checks, fsync and checkpoints are retained.
- An explicit 10 ms healthy interval and higher writer profile are optional;
  they do not bypass limits or the 5-second failure backoff. Start conservatively
  and measure live lag, memory, DB errors and confirmed drain rate.
- Runtime phase/pipeline/queue-wait metrics reset on restart; persisted job
  counters are the progress authority. No source paths/payloads are in status.
- No Go/native helper is required or shipped. An empty unchanged old `failed/`
  directory is tolerated; unknown/linked/nonempty directories remain for review.

The local 20k/100k fixtures and lazy 50M-name scanner demonstrate measured local
behavior and bounded algorithm storage. They do not prove a physical 50M-file
production import duration or production filesystem latency. See the acceptance
records linked in README for exact measurements and limitations.

Save configuration, source mounts and recoverable old artifacts before upgrade.
New imports resume only after review on restart; inspect checkpoints/quarantine
before increasing load. Controller instance-bound controls/source fencing require
the coordinated controller release; the archiver retains autonomous signal stop.
This document describes the candidate until its PR/tagged artifact is verified.
