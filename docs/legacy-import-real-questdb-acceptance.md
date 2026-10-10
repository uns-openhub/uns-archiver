# P4a: legacy import against real local QuestDB

Date: 2026-10-09. Candidate: `5efced6`, archiver 5.2.20, published kit 3.0.23.

## Contract and scope

Use disposable local Runtime infrastructure with exactly one controller.
Exercise the production `LegacyImportManager`, `writeLegacyEvent` and
`QuestDBWriter` with real HTTP ILP acknowledgements and independently queried
database rows. The fault proxy delays or rejects real requests; it never
fabricates a successful database acknowledgement.

| Scenario | Required result | Result |
| --- | --- | --- |
| Slow ILP flush | Pause answers while an accepted batch drains | PASS |
| Database unavailable | Rejected batch does not increase acknowledged file count | PASS; 64 deferred files |
| Importer restart | Persisted import stays paused until explicit resume | PASS |
| Recovery | Every fixture ID appears once with its original millisecond timestamp | PASS at 20,000 and 100,000 files |
| Abandoned `.processing` files | Included alongside ordinary `.event` files | PASS; one per 1,000 fixtures |
| Completion | Fresh verification traversal and no remaining source events | PASS |
| Operator status helper | Cached status stays responsive during scanning/writes | PASS; timings below are helper measurements |

All fixtures use a historical topic without consulting current active topics.
The table is provisioned with the existing production `time` designated column.
Append-mode automatic ILP table creation can instead designate `timestamp`;
automatic table provisioning is outside this drill. The API supports both
contracts separately; this runner deliberately tests preservation against the
existing `time` contract.

## Measured local evidence

The 100,000-file run verified 100,000 rows, zero duplicates, zero timestamp
mismatches and zero remaining source events. Recovery/import took **210.18 s**
with a deliberately bounded 64-file batch and 100 ms scheduling interval.
Pause during the slow flush answered in **6.98 ms**; cached status helper p99 was
**0.121 ms**, maximum event-loop delay **29.95 ms**, and maximum sampled process
RSS **202.52 MiB**. RSS includes the acceptance harness and fixture verification;
it is not a PM2 measurement of the full archiver application.

The unavailable phase rejected 21 HTTP attempts, deferred 64 files and left the
acknowledged count unchanged. After restart, explicit resume completed the import.
The slow batch acknowledged 64 rows before they were visible in the first SQL
query; all 64 became visible about **104 ms** later. Acknowledgement and query
visibility are separate evidence, and final success depends on database rows.

The earlier 20,000-file run independently verified all rows and timestamps.
Machine-readable evidence contains the final repeated 20,000-file run and the
100,000-file run: [evidence](evidence/legacy-import-real-questdb-2026-10-09.json).

Environment: Apple arm64, Node 24.11.0, 16 GiB host RAM, Podman VM approximately
3.79 GB RAM and five CPUs. Fresh named PostgreSQL/QuestDB volumes; no production
data. Infra containers used roughly 0.9 GB combined near the end of the large run.
The single controller uses candidate `c694570` / 2.1.144 with Setup migrations,
current postbuild GraphQL/assets and embedded UI. Local admin login and both direct
and public Caddy health routes returned 200; the UI route returned HTML. This is
availability evidence, not browser workflow acceptance.

## Reproduce

Start the disposable local Runtime using `uns runtime start --mode infra
--engine podman`, then run the commands documented in the archiver README.
The script accepts 512 to 100,000 synthetic files, fixes the database to
127.0.0.1:9000, creates a random fixture table and temporary directory, and
removes its own table/files after the run. Run sequentially for comparable
resource measurements. It deliberately resets no other data or volumes.

## Remaining acceptance

- Actual 5.2.17 to candidate MQTT handover, source exit/fence and later import
  through the authenticated runtime API and controller UI.
- Actual full-application SIGTERM/drain without a controller, and cluster
  restoration against real PostgreSQL locks and current broker traffic.
- API Global caller to HTTP to QuestDB correlation and query admission limits
  through the public proxy under slow database conditions (P2).
- Non-acknowledged and ambiguous database delivery, controlled writer contention,
  forced exit and reviewed recovery in the full application.
- Production filesystem/volume preflight and resource assessment; no extrapolated
  54-million-file runtime, native-helper requirement or exactly-once promise.

The true 5.2.17 source was separately extracted from tag
`ce8dcfc57c2b10b742f0381d2b0c688e7ed2d1d0`, installed with its frozen lock,
confirmed with core 3.0.16 and built for the next drill. It has not yet been
accepted through a live upgrade. Local data reset is authorized; production
retention and external tooling safeguards remain separate work.
