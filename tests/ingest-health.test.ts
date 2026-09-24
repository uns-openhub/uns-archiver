import assert from "node:assert/strict";
import test from "node:test";
import { assessIngestHealth, INGEST_BACKLOG_ALERT_EVENTS } from "../src/ingest-health.js";

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
  assert.match(health.message ?? "", /At least 1,000 events.*missing from history/);
  assert.match(health.action ?? "", /durable replay/);
});

test("does not claim healthy archive ingestion when backlog inspection fails", () => {
  const health = assessIngestHealth(null, checkedAt);
  assert.equal(health.state, "degraded");
  assert.match(health.message ?? "", /could not be inspected/);
});
