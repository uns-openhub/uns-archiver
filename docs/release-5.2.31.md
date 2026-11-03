# Archiver 5.2.31 — shared managed operator route registration

The candidate adopts the published SDK 3.0.43 core/API packages. Declared target
status, preview, pause and resume endpoints now automatically register their
actual API origin with the owning Controller through the SDK's shared lifecycle.
There is no Archiver-specific publisher, extra data grant or new control command.

Normal managed operator discovery requires the generic bootstrap prepared in
Controller 2.2.66. The host reuses an approved active service binding and injects
the exact current launch identity plus a private narrow route credential. The SDK
renews and validates its receipt; the host still proves unique live ownership
and the current operator workload grant before dispatch. Application policy
revisions, command receipts, pause/drain boundaries and database acknowledgements
remain Archiver-owned. Native operator and standalone behavior remain available.

Update participating hosts before this application's guarded upgrade. A host-only
hot update cannot add missing launch environment to an existing Archiver process.
Preserve its configuration, service policy, complete journal/file queue, volumes
and keys. Existing pauses or unknown effects remain migration blockers; never
remove their state to force a new launch. The standard source/target review must
choose and verify the actual placement before stopping a source.

Journal formats, runtime-state descriptors, MQTT paths/units, default bounds and
overall add-on compatibility are unchanged. No new backup-helper state contract
is required. Larger approved limits must be retained from the actual instance's
configuration; this version does not increase them automatically.

Verification against both the exact SDK 3.0.43 archives and an independent frozen
registry installation passed 226 tests, typecheck, build and the exact v5.2.31 tag
check. Two optional actual QuestDB/process fixture cases were not enabled in that
default run. Published SDK integrity and every archive file were verified against
the prepared source artifacts; the lock changes only the core/API versions and
integrities. Normally installed host acceptance, release publication and
production control remain separate pending gates.
This document records preparation, not a production pause or successful rollout.

After deployment, list operations, read targets.status, read an exact-table
targets.preview and compare policy revision/rules before and after. A preview
must not create a pause. It does not fence writers outside this Archiver, prove
complete history or establish replica visibility.
