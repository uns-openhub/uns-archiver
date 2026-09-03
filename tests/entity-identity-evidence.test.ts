import assert from "node:assert/strict";
import test from "node:test";
import { selectExactEntityIdentityEvidence } from "../src/entity-identity-evidence.js";
import type { ArchiverEntityBindingResolution } from "../src/entity-binding-client.js";

const topic = "site/line-a/press-14/equipment/main/temperature";
const exact: ArchiverEntityBindingResolution = {
  topic,
  asOf: "2026-09-03T10:00:00.000Z",
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

test("accepts only exact attribute binding evidence valid at event time", () => {
  assert.deepEqual(selectExactEntityIdentityEvidence(topic, "2026-09-03T10:00:00Z", exact), {
    status: "resolved",
    evidence: {
      stableEntityId: exact.stableEntityId,
      entityTypeKey: "openhub.asset",
      bindingRevision: "8",
      bindingDigest: exact.digest,
      resolution: "resolved",
      timeBasis: "source-event-time",
    },
  });
  assert.equal(selectExactEntityIdentityEvidence(topic, "2026-09-03T11:00:00Z", exact).status, "invalid");
});

test("rejects asset-prefix, ambiguous, and mismatched topic evidence", () => {
  assert.equal(selectExactEntityIdentityEvidence(topic, "2026-09-03T10:00:00Z", {
    ...exact,
    bindingKind: "asset-prefix",
    matchedPath: "site/line-a/press-14",
  }).status, "inexact");
  assert.equal(selectExactEntityIdentityEvidence(topic, "2026-09-03T10:00:00Z", {
    ...exact,
    status: "ambiguous",
    stableEntityId: null,
  }).status, "ambiguous");
  assert.equal(selectExactEntityIdentityEvidence(topic, "2026-09-03T10:00:00Z", {
    ...exact,
    matchedPath: "site/line-b/press-14/equipment/main/temperature",
  }).status, "inexact");
});
