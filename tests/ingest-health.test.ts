import assert from "node:assert/strict";
import test from "node:test";
import {
  assessIngestHealth,
  INGEST_BACKLOG_ALERT_EVENTS,
} from "../src/ingest-health.js";

const checkedAt = "2026-09-24T08:00:00.000Z";

test("marks a bounded durable backlog as healthy", () => {
  assert.deepEqual(assessIngestHealth(12, checkedAt), {
    id: "archiver-ingest",
    label: "Archive ingest",
    state: "healthy",
    healthy: true,
    checkedAt,
    storedQueuedLowerBound: 12,
  });
});

test("reports delayed history when the durable backlog reaches the alert limit", () => {
  const health = assessIngestHealth(INGEST_BACKLOG_ALERT_EVENTS, checkedAt);
  assert.equal(health.state, "degraded");
  assert.equal(health.healthy, false);
  assert.match(
    health.message ?? "",
    /At least 1,000 events.*missing from history/,
  );
  assert.match(health.action ?? "", /durable replay/);
});

test("does not claim healthy archive ingestion when backlog inspection fails", () => {
  const health = assessIngestHealth(null, checkedAt);
  assert.equal(health.state, "degraded");
  assert.match(health.message ?? "", /could not be inspected/);
});

test("partial, stale, or processing-only snapshots cannot claim fresh empty history", () => {
  const complete = {
    scanComplete: true,
    processingFiles: 0,
    capturedAt: new Date().toISOString(),
    lastError: null,
  };
  assert.equal(assessIngestHealth(0, undefined, complete).healthy, true);
  assert.equal(
    assessIngestHealth(0, undefined, { ...complete, scanComplete: false })
      .healthy,
    false,
  );
  assert.equal(
    assessIngestHealth(0, undefined, { ...complete, capturedAt: checkedAt })
      .healthy,
    false,
  );
  assert.equal(
    assessIngestHealth(0, undefined, { ...complete, processingFiles: 1 })
      .healthy,
    false,
  );
  assert.equal(
    assessIngestHealth(null, undefined, {
      ...complete,
      lastError: "scan failed",
    }).healthy,
    false,
  );
});

test("this replay's known active writer does not degrade a complete small-backlog inspection", () => {
  const inspection = {
    scanComplete: true,
    processingFiles: 1,
    unresolvedProcessingFiles: 0,
    capturedAt: new Date().toISOString(),
    lastError: null,
  };
  assert.equal(assessIngestHealth(0, undefined, inspection).healthy, true);
  assert.equal(
    assessIngestHealth(0, undefined, {
      ...inspection,
      unresolvedProcessingFiles: 1,
    }).healthy,
    false,
  );
});
