# Pause queue replay for same-host maintenance

The operator `replay.pause` command stops new replay writes from the target
SQLite journal, event files and legacy imports. MQTT admission continues while
storage budgets allow. Already accepted writes finish; read `replay.status`
until `boundary.phase` is `paused` and `inFlight` is zero. A successful pause
command alone does not prove this boundary.

This control is separate from the existing global `control?action=pause`, which
routes new input into event files, and from a table-specific target pause.
Selective table rules are preserved. Replay resume does not clear them.

The replay rule, revision and latest 128 command receipts are persisted atomically
with file and parent-directory fsync in private `archiver-replay-control.json`
inside the instance. Corrupt or unsafe state blocks startup; a persistence error
blocks further replay in the current process. Repeated identical request IDs
return their original receipt. Changed payloads/actors or stale revisions are
rejected. After receipt eviction, an old revision is still rejected.

## Use through the existing MCP operator workflow

1. Discover the live controller, package and instance. Call
   `controller_list_runtime_operator_operations` for that exact selector.
2. Read `replay.status` with `controller_read_runtime_operator_operation` and
   `payload: {}`. Preserve the observed revision.
3. Review `replay.pause` using this payload, a newly generated UUID and the
   observed revision:

   ```json
   {
     "action": "pause",
     "requestId": "<new UUID>",
     "expectedRevision": 0,
     "reason": "Reviewed same-host maintenance"
   }
   ```

4. Execute the reviewed command with its returned ID/checksum and `CONFIRM`.
   Read status to confirm the settled boundary. On response loss, reconcile the
   same command ID, actor and payload; do not create another operation.
5. Use normal Controller Stop. Verify natural exit 0, the physical PID is gone
   and the exclusive SQLite owner lease is released before touching storage.
6. Reuse the **whole current instance directory** on the same mounted filesystem,
   including `event_storage` (SQLite main/WAL and files), replay control,
   configuration and receipts. Verify the same physical storage and destination.
   Do not copy a live SQLite file, attach an empty target, or allow two owners.
   Normal migration continues to reject retained queues and pauses. This control
   does not add automatic queue transfer or a cross-host storage migration.
7. Start the successor once. Read its actual version, input readiness, replay
   revision and paused boundary. The durable pause survives restart; pending
   records remain held. A lost Start response requires observing the running
   PID, because submitting Start again can restart it.
8. Review/execute `replay.resume` with a **new** UUID, the current revision and
   `action: "resume"`. Verify write acknowledgements, queue direction and
   separately the requested history windows. A full inspection is needed to
   prove an empty file queue; partial counts remain lower bounds.

This is a controlled Stop/Start upgrade, with a possible MQTT subscription gap.
Only already accepted durable data is covered without broker replay evidence.
A database commit with a lost acknowledgement can repeat a row in append mode.
Returning to an older binary that does not understand replay control requires an
explicit review: it will not honor this new pause. Do not restore an old SQLite
snapshot after acknowledged successor writes.

## Why retain both storage mechanisms?

The established global pause and overflow paths store complete MQTT envelopes
in files. The target journal stores resolved table parts, target pause policy,
ordered work and confirmation receipts in SQLite, using synchronous FULL.
A file count is not comparable to a target-row count; one packet can have several
parts and retained source/receipt records can overlap.

SQLite supports atomic packet admission, per-target scheduling, bounded replay,
crash recovery and table-policy revisions without a separate metadata journal.
Files can be quick to append but still need those guarantees, directory scans,
claiming/recovery and acknowledgement bookkeeping. Neither representation alone
proves greater end-to-end throughput. The 5.2.32 change fixed an inefficient
SQLite receipt cleanup index; its microbenchmark is not a production replay
benchmark. This change keeps all queue formats and accepted records compatible.
Any future storage replacement must be measured under equal durability and
recovery guarantees, and deployed after the existing retained queue is drained
or through a separately reviewed importer.

## Retained-store adoption in 5.2.33

The package manifest declares the event directory and optional replay, target
and import control files. An attached store checks its owner and generation
before opening the journal. Its first startup durably pauses replay for that
adoption operation. Input continues within the configured disk limits.
A reviewed resume persists across restart.

With Controller 2.2.69 and Runtime 2.2.69, an operator can review a verified
copy from a stopped instance into the external Runtime volume. A successful
transfer leaves the original copy available and fences the original owner.
Returning the store transfers the current queue and control files, including
work accepted since the transfer. After interruption, reconcile the original
review and operation ID before starting either owner.

Archiver 5.2.33 pins UNS Kit core and API to 3.0.46. The existing SQLite
and file queue formats stay compatible with the declared source versions.
An older binary may interpret replay control differently after return.
