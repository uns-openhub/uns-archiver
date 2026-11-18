# Durable journal group commits

The candidate keeps SQLite WAL and synchronous=FULL, the current queue schema,
pause policy and retained-store format. Concurrent operations share a commit in
cohorts of at most 128. Callers cannot acknowledge a sample before commit.

Local checks: 242 tests passed, three opt-in checks skipped; typecheck and build
passed. Coverage includes partial/failed commits, queued-admission close/reopen,
SIGKILL with an uncertain writer, pause/resume, capacity errors, duplicate IDs,
retained source files and normal recovery.

A synthetic local journal benchmark admitted and wrote all 4,096 samples with no
pending work. The released implementation used 16,384 transactions and the
candidate used 128. On this machine the measured rates were 2,939 and 6,267
samples/s, respectively. These rates use a synthetic downstream writer and are
not production or QuestDB throughput claims.

Reproduce after building both revisions:

```sh
UNS_ARCHIVER_BENCHMARK=1 \
BASELINE_MODULE=/absolute/baseline/dist/target-journal.js \
node scripts/benchmark-target-journal.mjs
```

An isolated MQTT/Python-consumer/Archiver/QuestDB integration passed with 218
synthetic source rows and four verified result rows. It includes late input,
metadata discovery, abrupt consumer restart, source/output readback, duplicate
suppression and a missing-input job remaining pending.

Production acceptance remains separate: preserve every accepted queue and
receipt, prove old-process exit, retain the source store, adopt it under the
reviewed owner, then verify health, backlog drain and history read visibility.
No queue purge or durability relaxation is part of this change.

An additional two-version process acceptance passed from 5.2.35 to 5.2.36:
eight process generations, a scoped local HTTP outage, lost replies, retained
SQLite and JSON queues, adoption of the same physical store, paused receiver
restart, return to the original owner, 23 exact result rows and natural
cooperative Stop (exit 0). The test waits for application bootstrap, including
shutdown-handler installation, before testing Stop. Its cleanup forcibly closes
only its own MQTT client so a test failure cannot hide behind an open session.
