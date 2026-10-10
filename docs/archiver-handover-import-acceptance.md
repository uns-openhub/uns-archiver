# P4b: real archiver handover and explicit legacy import

Date: 2026-10-09. Local, disposable infrastructure only. Controller 2.1.144
candidate based on `c694570`; archiver 5.2.20 candidate based on `79d6dba`.
Old source is actual tag v5.2.17 (`ce8dcfc`), frozen dependencies / Kit 3.0.16;
new runtime uses published Kit 3.0.23. No version/tag/publication change.

## Contract

One controller, real MQTT broker, PostgreSQL, QuestDB and Podman Caddy.
Install isolated built artifacts into RTT version directories, start through
ordinary authenticated GraphQL, use actual MQTT handover, then explicitly import
from the old instance through the authenticated runtime API and controller UI.
This checks runtime lifecycle, not catalog installation or production upgrade.

| Scenario | Observed result |
| --- | --- |
| Old baseline | 100 events became real QuestDB rows |
| Old paused backlog and handover | 400 old files retained; old process exited 0, restarts 0, desiredRunning false |
| Restart old source | Controller rejected start using durable source fence |
| New live traffic | Events after handover and during legacy import reached QuestDB |
| Auth/command guards | Missing auth 401; stale owner/revision 409; absent closure confirmation and arbitrary path 400 |
| Old abandoned processing file | One deliberately renamed old file imported alongside ordinary events |
| UI workflow | Inspect, reviewed old-source closure, Start import, Resume after fix; completed 400 files/rows |
| Restart persistence | Completed job revision 2 retained; no reimport; old dialog detects changed runtime owner |
| Autonomous stop | Controller absent; candidate SIGTERM drained its 8 pending rows and exited 0; PM2 stayed stopped, restart count unchanged |
| Independent SQL oracle | 800 rows, 800 unique IDs, zero missing/duplicate IDs or historical time mismatches |
| Old residual spool | Zero queued/processing events; empty failed directory retained |

The handover publisher sent 300 events over 15 seconds, but old ownership was
released after publication ended. Separate new-owner traffic and overlapping
import traffic were verified. This does **not** prove continuous, lossless packet
transfer exactly across the ownership boundary, or exactly-once delivery.
Shutdown detection was sampled every 100 ms; the observed approximately 100 ms
exit is not a hard drain latency guarantee. Controller startup restored the
candidate's desired process; the old version remained stopped.

## Defects found and fixed

1. Dynamic Caddy routes pointed to the controller's advertised loopback URL
   inside Podman. Resolve only the exact local controller origin through
   `server.routeHost`, preserving paths and leaving peer/unrelated URLs intact.
   Both incremental and complete route replacement use the resolver.
2. Old 5.2.17 creates `event_storage/failed` even when empty. The importer formerly
   blocked after successfully importing every event. Only an unchanged real
   empty `failed` directory is tolerated, without deletion. Nonempty or linked
   directories still remain blocked for review. Reads one entry, not a recursive
   inventory of the failed directory.

Controller focused routing tests: **86 passed**, typecheck passed. Archiver full
verify: **151 passed**, typecheck and build passed. Added scanner script also
passes standalone strict typecheck with explicit Node types.

## Fifty million files: what is proven

The production scanner was exercised with a lazy iterator of 50,000,000 names:
97,657 passes, maximum 512 entries/pass, early close after 512 entries, all names
visited. Maximum sampled harness RSS approximately 202.12 MiB. Its 12.85-second
runtime measures generated names, **not physical directory IO, file parsing,
QuestDB throughput, or 50-million-file acceptance**. The separate P4a real-file /
QuestDB run is bounded to 100,000 files.

New code retains a cursor and applies a cooperative 25 ms scan budget, bounds
batch/concurrency/bytes/queue sizes, prioritizes live traffic, serves cached
inspection with explicit unknown/observed counts, and removes files after DB
acknowledgement. Filesystem awaits can exceed that budget. Pause stops new work;
already accepted work drains. Ambiguous acknowledgements can replay duplicates.

Old 5.2.17 already streams directory entries, so it does not load 50 million
names into RAM. However startup awaits full stale-processing recovery before
MQTT activation, and some status/control paths count the entire directory.
Restart and operator requests can take a very long time. It lacks the new
bounded scanning/status protections; this drill does not modify old code.

## Production gates still open

- Measure the actual filesystem or a representative server volume: increasing
  real-file counts, directory/rename/unlink latency, disk/inodes, RSS, HTTP/status
  latency, live MQTT delay and QuestDB backpressure/outage recovery. Avoid a full
  upfront count/copy/move on the production spool. Import in place from the
  configured local allowlist, after confirming the source writer has exited.
- Confirm the existing old process launcher. The local old runtime was started
  under the candidate controller preload. A production process launched before
  that protection existed cannot acquire it live. Choose and test a reviewed
  cold source-closure path or a guarded launch before claiming equivalent hot
  handover safety. Do not silently restart an old 50M-file process to inject it.
- Production retained-volume/uninstall protection and real PostgreSQL cluster
  restoration remain separate gates. Keep old source volume/version until
  import and retained error-file review finish.
- API Global real slow-query/caller correlation acceptance remains P2.
- Consider segmented/sharded future spools or a native helper after measuring
  the bottleneck; they do not eliminate old directory or database IO costs.

Machine-readable [evidence](evidence/archiver-handover-import-2026-10-09.json).
UI screenshot is a local acceptance artifact at
the private local staging directory (not part of the published repository).
