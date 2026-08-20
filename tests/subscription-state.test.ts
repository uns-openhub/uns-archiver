import assert from "node:assert/strict";
import test from "node:test";
import { canonicalizeTopics, subscriptionDelta } from "../src/subscription-state.js";

test("canonical active topics ignore ordering, duplicates, and trailing separators", () => {
  assert.deepEqual(
    canonicalizeTopics(["sij/metal/#", "sij/#/", "sij/metal/#", "solvera/#"]),
    ["sij/#", "sij/metal/#", "solvera/#"],
  );
});

test("subscription reconciliation only changes actual MQTT filters", () => {
  assert.deepEqual(subscriptionDelta(["sij/#", "solvera/#"], ["solvera/#", "sij/#"]), {
    subscribe: [],
    unsubscribe: [],
    next: ["sij/#", "solvera/#"],
  });

  assert.deepEqual(subscriptionDelta(["sij/#", "solvera/#"], ["sij/#", "sij/metal/#"]), {
    subscribe: ["sij/metal/#"],
    unsubscribe: ["solvera/#"],
    next: ["sij/#", "sij/metal/#"],
  });
});
