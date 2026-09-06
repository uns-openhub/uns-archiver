import assert from "node:assert/strict";
import test from "node:test";
import {
  isNewerBindingRevision,
  parseEntityBindingInvalidation,
} from "../src/entity-binding-invalidation.js";

const validPayload = {
  schemaVersion: 1,
  eventType: "entity-observation-binding-revision",
  scopeKey: "tenant:default",
  transitionId: "11111111-1111-4111-8111-111111111111",
  bindingRevision: "42",
  topicPaths: ["site/line-a/press-14", "site/line-b/press-14/temperature"],
};

test("parses and deduplicates a concrete binding invalidation", () => {
  assert.deepEqual(parseEntityBindingInvalidation({
    ...validPayload,
    topicPaths: [...validPayload.topicPaths, validPayload.topicPaths[0]],
  }), {
    scopeKey: "tenant:default",
    transitionId: validPayload.transitionId,
    bindingRevision: "42",
    topicPaths: validPayload.topicPaths,
  });
});

test("rejects unknown versions, wildcard topics, and invalid revisions", () => {
  assert.equal(parseEntityBindingInvalidation({ ...validPayload, schemaVersion: 2 }), null);
  assert.equal(parseEntityBindingInvalidation({ ...validPayload, topicPaths: ["site/#"] }), null);
  assert.equal(parseEntityBindingInvalidation({ ...validPayload, bindingRevision: "0" }), null);
});

test("accepts only strictly newer revisions", () => {
  assert.equal(isNewerBindingRevision("42", null), true);
  assert.equal(isNewerBindingRevision("42", "41"), true);
  assert.equal(isNewerBindingRevision("42", "42"), false);
  assert.equal(isNewerBindingRevision("42", "43"), false);
});
