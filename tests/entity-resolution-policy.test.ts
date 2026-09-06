import assert from "node:assert/strict";
import test from "node:test";
import {
  decideEntityResolution,
  resolvedEnvelopeEvidence,
} from "../src/entity-resolution-policy.js";
import type { ArchiverEntityBindingResolution } from "../src/entity-binding-client.js";

const topic = "site/line-a/press-14/equipment/main/temperature";
const eventTime = "2026-09-03T10:00:00.000Z";
const exact: ArchiverEntityBindingResolution = {
  topic,
  asOf: eventTime,
  status: "resolved",
  stableEntityId: "11111111-1111-4111-8111-111111111111",
  entityTypeKey: "openhub.asset",
  bindingKind: "attribute-topic",
  matchedPath: topic,
  validFrom: "2026-09-03T09:00:00.000Z",
  validTo: "2026-09-03T11:00:00.000Z",
  timeBasis: "source-event-time",
  sourceCount: 1,
  revision: "8",
  digest: `sha256:${"1".repeat(64)}`,
};

test("persists and reuses the accepted exact identity decision", () => {
  const first = decideEntityResolution(
    { topic, eventTime, resolution: exact },
    { now: "2026-09-03T10:00:01.000Z", retryMaxAgeMs: 30_000 },
  );
  assert.equal(first.action, "enriched-write");
  assert.ok(first.envelope);
  const replay = decideEntityResolution(
    {
      topic,
      eventTime,
      persistedEnvelope: first.envelope,
      controllerError: new Error("offline"),
    },
    { now: "2026-09-03T12:00:00.000Z", retryMaxAgeMs: 30_000 },
  );
  assert.equal(replay.action, "enriched-write");
  assert.equal(replay.evidence?.bindingRevision, "8");
});

test("rejects a persisted decision for another topic or event time", () => {
  const decision = decideEntityResolution(
    { topic, eventTime, resolution: exact },
    { now: eventTime, retryMaxAgeMs: 30_000 },
  );
  assert.equal(
    resolvedEnvelopeEvidence(decision.envelope, `${topic}/other`, eventTime),
    null,
  );
  assert.equal(
    resolvedEnvelopeEvidence(
      decision.envelope,
      topic,
      "2026-09-03T10:00:01.000Z",
    ),
    null,
  );
});

test("retries controller outages for a bounded time then writes legacy", () => {
  const first = decideEntityResolution(
    { topic, eventTime, controllerError: new Error("offline") },
    { now: "2026-09-03T10:00:01.000Z", retryMaxAgeMs: 30_000 },
  );
  assert.equal(first.action, "retry");
  const pending = decideEntityResolution(
    {
      topic,
      eventTime,
      controllerError: new Error("offline"),
      persistedEnvelope: first.envelope,
    },
    { now: "2026-09-03T10:00:20.000Z", retryMaxAgeMs: 30_000 },
  );
  assert.equal(pending.action, "retry");
  const expired = decideEntityResolution(
    {
      topic,
      eventTime,
      controllerError: new Error("offline"),
      persistedEnvelope: first.envelope,
    },
    { now: "2026-09-03T10:00:31.000Z", retryMaxAgeMs: 30_000 },
  );
  assert.deepEqual(expired, {
    action: "legacy-write",
    reason: "retry-expired",
    evidence: null,
    envelope: null,
  });
});

test("writes authoritative unresolved and inexact results through legacy compatibility", () => {
  assert.equal(
    decideEntityResolution(
      {
        topic,
        eventTime,
        resolution: { ...exact, status: "not-found", stableEntityId: null },
      },
      { now: eventTime, retryMaxAgeMs: 30_000 },
    ).action,
    "legacy-write",
  );
  const inexact = decideEntityResolution(
    {
      topic,
      eventTime,
      resolution: {
        ...exact,
        bindingKind: "asset-prefix",
        matchedPath: "site/line-a/press-14",
      },
    },
    { now: eventTime, retryMaxAgeMs: 30_000 },
  );
  assert.equal(inexact.action, "legacy-write");
  assert.equal(inexact.reason, "inexact");
});
