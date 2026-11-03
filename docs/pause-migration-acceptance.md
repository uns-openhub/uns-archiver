# Pause blocks Archiver handover

Applies to Archiver 5.2.29 and Controller 2.2.61. The local acceptance below was
performed with candidates based on Archiver 5.2.28 and Runtime 2.2.60, before
source publication. UNS Kit remains published 3.0.41. No configuration schema,
SDK, backup helper or production state was changed by this implementation.
Archiver declares `controllerCompatibility: >=2.2.61 <3` so older Controllers
cannot install it without the matching generic migration guards.

## Operator rule

A global pause or any selective table/topic pause blocks migration, including
an empty queue and a rule for an unobserved topic. Cold mode must not bypass
this condition. Resume every pause through the normal control API, allow
retained writes to finish, and request a fresh migration preview. Paused queues
are not transferred.

The application checks global/selective pause during SDK export, validation and
import. An already retained or incoming paused policy cannot replace state.
Initial source validation runs before ownership release. Final validation after
drain accepts only the exact cached final policy, without reading closed SQLite.
The target still requires an open, healthy and empty journal. Normal Stop/Start
retains selective rules and queue; the legacy global pause remains process-local.

The derived policy file remains present while paused, so generic Controller
preflight can reject the retained state before changing either process. Its
manifest and format are unchanged. Resume removes only the derived file; SQLite
retains canonical policy revisions and command receipts. The generic Controller
candidate accepts matching optional descriptors with both snapshots absent and
fully proven SDK capabilities. It blocks retained files for SDK-v2 sources,
including cold-mode requests. Legacy file runtimes without that capability keep
their existing guarded cold transfer. No Controller execution branch uses an
application name.

## Drain and ownership

One SDK process owns the input proxy, API proxy and metadata. Passive successors
cannot accept control writes or advertise active routes. Readiness is advertised
only after input handlers and API are prepared. The input-only proxy may flush
SDK housekeeping publications during stop without activating input or DB writes.

Handover drains accepted application work while keeping the SDK control
connection open for the successor acknowledgement. Full signal cleanup closes
that connection. Release also stops application maintenance timers, allowing
ordinary cooperative Stop to exit naturally after SDK cleanup.

Preliminary validation permits healthy writes already in flight to finish during
drain. Pending retries, unstarted journal work, storage failure and incomplete or
nonzero file-queue inspection remain blocking. Final export after input freezes
requires confirmed drain. No SQLite data or overflow files are copied or deleted
to achieve a migration.

## Local evidence

The managed drill used two actual Controller containers from released Runtime
2.2.60 with the candidate Archiver build and two generic Controller modules
overlaid explicitly. It did not modify a published image or pretend to test
unchanged release artifacts. Existing volumes, keys and configurations were
retained. All records and identities were disposable synthetic local fixtures.

The drill confirmed:

- Global and table pauses reject hot and cold previews. A pause after review
  rejects the stale selection before the source PID changes or target starts.
- Hot migration and reverse migration to a fresh prepared placement complete
  with acknowledged SDK state and clean source exit. The old source fence stays.
- A deliberately discarded migration HTTP response is reconciled through the
  same durable operation. Repeating Resume does not start another target.
- Normal Controller Stop/Start restores the public API route and policy revision.
- A physically stopped local QuestDB retains writes for both tables without
  restarting the source, then recovery confirms all retained records.
- An additional hot move under continuous input (nominally 20 rows/s) completes.
  Across the drill all 448 records, 224 per table, match time, value, unit and
  topic exactly. The final active queue and in-flight counts are zero.

Separate process integration uses real MQTT/QuestDB and SDK JWKS with a scoped
discovery fixture. It covers a paused table, healthy tables, DB transport failure,
SIGKILL/restart with retained policy, lost pause response, denied access, a passive
successor's 503, signal cleanup and launch-bound cooperative Stop. This fixture
is separate from the actual managed two-Controller drill.

Unit/regression coverage also exercises closed-journal validation, rules for
unobserved topics, incoming/retained pauses, repeated SDK requests before release,
healthy in-flight writes versus retries, disk limits, uncertain acknowledgements,
and generic retained-file/legacy runtime compatibility. All 222 Archiver tests
passed sequentially, including both opt-in real-service tests; typecheck and build passed. A final 29-test regression also checks that
the file scan started strictly after the last durable spill. The generic
Controller acceptance covered 210 migration/runtime-state tests. Final release
checks are recorded separately from this development-stage drill.

## Limits and next release boundary

Both Controllers ran in one local Linux VM, not independent-host HA. The modest
continuous load is a functional drill, not a throughput or memory benchmark.
The entry path was the authenticated normal GraphQL management API and public
service proxy; no browser visual acceptance is claimed.

Lost command response and lost database acknowledgement are different. A DB
commit with a lost acknowledgement can still repeat in append mode; these tests
do not establish universal exactly-once delivery or replay of messages the broker
never delivered. A failure after ownership freezes still requires reconciliation
of the same operation and preserved owners.

Backup/restore of the selective journal was not rerun here; spool-excluding
backups omit that queue. The existing manifest remains, but helper discovery does
not automatically collect this root-level policy mirror. No new backup coverage
is claimed.

Source publication and the signed catalog follow the normal separately approved
release workflow after full release checks. No new SDK or Tools release is
required by these changes. Production activation remains a separate approved step.
