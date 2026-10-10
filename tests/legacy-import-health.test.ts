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
