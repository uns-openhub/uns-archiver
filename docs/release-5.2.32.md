# UNS Archiver 5.2.32

Operator reads can intermittently fail when a short JWKS transport outage occurs.
This release adopts the published `@uns-kit/api` and `@uns-kit/core` 3.0.44
packages. The shared verifier uses bounded/coalesced key retrieval recovery and
preserves signature, expiry, launch, audience, request and online parent checks.
It does not retry an application effect or extend trust in an expired key.

After a selective table pause, receipt maintenance previously inspected pending
payload rows while looking for expired live receipts. A partial index now holds
only confirmed live receipt sequence numbers; maintenance selects the older
receipt primary keys from that index. It retains the newest 10,000 live receipts,
all unacknowledged legacy receipts, every pending/retrying row and the pause
policy. Existing version-1 journals gain an additive index when opened. Queue
budgets, FULL durability, writer batching and replay scheduling are unchanged.

## Verification

- Complete serial `pnpm run verify`: 227 passed, two explicitly optional
  external-fixture skips, zero failures; typecheck and build passed.
- The 21 journal tests include reopening an issued v1 schema without the new
  index, retained pause/retry payloads, 10,005 live receipts, legacy receipts,
  duplicate submission, another restart and exact synthetic resume delivery.
- Actual published API package, loopback operator HTTP/JWKS and a real SQLite
  journal cover a transient JWKS 503, revocation, unavailable current authority
  and successful restoration without changing the pause or queued rows.
- Coordinated Controller component acceptance passed 45 actual local
  MCP/GraphQL/PostgreSQL/SDK/Archiver cases without skips, including connection
  reset, slow JWKS headers/body, persistent outage, recovery and denied access.
- A separate synthetic 100,000-row pending backlog with 10,000 live and 20 legacy
  receipts took about 743 ms before and 2 ms after for 20 maintenance iterations.
  No row counts changed. This measures receipt cleanup only, not total replay
  speed or production performance.

The external QuestDB/full-process MQTT fixture cases were not rerun for this
release. Component tests do not imply a normally installed multi-host acceptance
or an exactly-once database guarantee.

## Compatibility and rollout

The existing `>=2.2.61 <3` Controller compatibility range is unchanged. No new
host capability, application-specific Controller rule or SDK contract is added.
Managed operator registration retains its existing launch/bootstrap prerequisites.
Normal deployment still requires a separately reviewed upgrade and current owner
checks; publishing this source release does not restart a service, remove a pause,
change caller rights or discard an accepted queue.
