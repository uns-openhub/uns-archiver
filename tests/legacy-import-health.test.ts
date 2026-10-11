import assert from "node:assert/strict";
import test from "node:test";
import { assessLegacyImportHealth } from "../src/legacy-import-health.js";

const source = (state?: string, error: string | null = null, empty = false) =>
  ({
    sourceId: "retired",
    loaded: true,
    error,
    ownership: "unclaimed",
    quarantinePresent: null,
    drainRate: {
      acknowledgedFilesPerSecond: null,
      rowsPerSecond: null,
      measuredAt: null,
    },
    inspection: {
      scanComplete: empty,
      events: empty ? 0 : null,
      processing: empty ? 0 : null,
      other: empty ? 0 : null,
      countKind: empty ? "observed" : "unknown",
      capturedAt: null,
      error: null,
    },
    job: state ? { state } : null,
  }) as any;

test("no configured legacy recovery adds no dependency alert", () => {
  assert.equal(assessLegacyImportHealth([]), null);
});
test("unfinished or unavailable recovery degrades health even with an empty live spool", () => {
  for (const state of [
    undefined,
    "running",
    "paused",
    "blocked",
    "cancelled",
  ]) {
    assert.equal(assessLegacyImportHealth([source(state)])?.healthy, false);
  }
  assert.equal(
    assessLegacyImportHealth([source("completed", "source-io-unavailable")])
      ?.healthy,
    false,
  );
});
test("confirmed completion or a successfully inspected empty source clears the recovery warning", () => {
  assert.equal(assessLegacyImportHealth([source("completed")])?.healthy, true);
  assert.equal(
    assessLegacyImportHealth([source(undefined, null, true)])?.healthy,
    true,
  );
  assert.equal(
    assessLegacyImportHealth([source("completed"), source("paused")])?.healthy,
    false,
  );
});

test("running recovery remains visible without suggesting the live writer is unavailable", () => {
  const health = assessLegacyImportHealth([source("running")]);
  assert.equal(health?.recoveryState, "running");
  assert.equal(health?.healthy, false);
  assert.match(health?.message ?? "", /is running/);
  assert.match(health?.action ?? "", /Monitor/);
  assert.doesNotMatch(health?.action ?? "", /starting or resuming/);
});

test("paused, pending and blocked sources have distinct operator actions", () => {
  assert.equal(assessLegacyImportHealth([source("paused")])?.recoveryState, "paused");
  assert.equal(assessLegacyImportHealth([source()])?.recoveryState, "pending");
  for (const state of ["blocked", "cancelled"])
    assert.equal(assessLegacyImportHealth([source(state)])?.recoveryState, "blocked");
  const stopped = source("paused"); stopped.job.lastError = "shutdown-review-required";
  assert.equal(assessLegacyImportHealth([stopped])?.recoveryState, "paused");
  stopped.job.lastError = "restart-review-required";
  assert.equal(assessLegacyImportHealth([stopped])?.recoveryState, "paused");
  stopped.job.lastError = "source-ownership-lost";
  assert.equal(assessLegacyImportHealth([stopped])?.recoveryState, "blocked");
});

test("running source never masks a paused, unknown, failed or quarantined peer", () => {
  assert.equal(assessLegacyImportHealth([source("running"), source("paused")])?.recoveryState, "paused");
  assert.equal(assessLegacyImportHealth([source("running"), source()])?.recoveryState, "pending");
  assert.equal(assessLegacyImportHealth([source("running"), source("completed", "source-io-unavailable")])?.recoveryState, "blocked");
  for (const field of ["quarantined", "deferred"]) {
    const warning = source("completed"); warning.job[field] = 1;
    assert.equal(assessLegacyImportHealth([source("running"), warning])?.recoveryState, "blocked");
  }
  const quarantined = source("completed"); quarantined.quarantinePresent = true;
  assert.equal(assessLegacyImportHealth([quarantined])?.recoveryState, "blocked");
  const unloaded = source("running"); unloaded.loaded = false;
  assert.equal(assessLegacyImportHealth([unloaded])?.recoveryState, "pending");
});
