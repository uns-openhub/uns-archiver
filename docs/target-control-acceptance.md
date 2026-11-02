# Selective archive target control — local acceptance

Scope: Archiver 5.2.28, based on 5.2.27, with local acceptance evidence. No
production deployment or production table pause was performed. Existing UNS Kit
3.0.41 contracts are reused; this change does not require an SDK publication or
application-specific Controller logic.

## Integrated process check

The optional `tests/archiver-process.integration.test.ts` starts the actual built
Archiver from an isolated instance directory. It uses a disposable MQTT broker,
QuestDB, real SDK JWKS validation, and authenticated synthetic topic discovery.
The discovery/JWKS fixture is deliberately bounded; it is not a Controller-managed
installation, UI, or public proxy acceptance test.

The completed local run on 3 November 2026 verified:

- Topic discovery and the Archiver's actual service-endpoint registrations.
- Anonymous 401, reader 403, passive-owner mutation 409, and operator commands.
- Correlated-v2 SDK startup with an explicit raw-input state contract.
- An in-flight database write held while a pause response was abandoned. Retrying
  the same request recovered its recorded operation and still reported draining.
- Independent data/table destinations: six rows for the paused/resumed data table,
  four for another data table, and four for the table-packet destination.
- An actual stop/start of the disposable local QuestDB container. All three tables
  retained input; the Archiver was forcibly stopped and reopened while the database
  remained unavailable, then received more MQTT input.
- Automatic replay of unpaused tables after recovery; the selected table stayed
  queued until explicit operator resume. Repeating resume did not repeat rows.
- Exact read-back of all 14 timestamps, numeric values, units and source topics,
  including equal repeated MQTT payloads. Final target backlog was zero.
- Clean ordinary process shutdown after recovery.

## Regression and state boundaries

All 210 tests passed in a serial combined run, including both opt-in real-database
checks. Typecheck, build and whitespace checks passed. Additional tests cover
SQLite write/quota rejection, a single journal owner, byte-bounded replay, stale
revision, late writer completion, durable state after SIGKILL, retained legacy
source files and mixed-packet receipts, and transfer from old stored-event files.

A separate read-only integration with the Controller's existing generic
runtime-state functions accepted this manifest for an arbitrary application name,
copied a pause into a missing target state, preserved its revision, allowed an
identical retry and rejected a changed target. No Controller source was modified.
That checks the shared file contract; it does not claim a two-controller live
handover. Pending target data blocks SDK policy export rather than being dropped
or silently copied.

## Limits and production gates

Only this Archiver owner's writes are controlled. External writers are outside
its authority. Flush acknowledgements do not prove WAL application, replica
visibility or database-wide writer exclusion. Lost acknowledgements can repeat
append-mode rows; no exactly-once or unlimited-retention guarantee is made.

Before production activation, review and release the candidate, verify its normal
Controller-managed installation and route, measure ingress and retention capacity,
and agree the unlock/unknown-outcome procedure. A real two-controller handover of
this candidate remains a separate acceptance step. Do not remove or bypass a
retained queue to make a handover pass. The portable policy is not a queue copy.
