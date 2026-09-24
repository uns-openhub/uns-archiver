export const INGEST_BACKLOG_ALERT_EVENTS = 1_000;

export type IngestHealth = {
  id: "archiver-ingest";
  label: "Archive ingest";
  state: "healthy" | "degraded";
  healthy: boolean;
  checkedAt: string;
  message?: string;
  action?: string;
  storedQueuedLowerBound?: number;
};

export function assessIngestHealth(
  storedQueuedLowerBound: number | null,
  checkedAt = new Date().toISOString(),
): IngestHealth {
  const base = {
    id: "archiver-ingest" as const,
    label: "Archive ingest" as const,
    checkedAt,
  };

  if (storedQueuedLowerBound === null) {
    return {
      ...base,
      state: "degraded",
      healthy: false,
      message: "The durable event backlog could not be inspected; archive freshness is unknown.",
      action: "Inspect the archiver event storage directory and its filesystem permissions.",
    };
  }

  if (storedQueuedLowerBound >= INGEST_BACKLOG_ALERT_EVENTS) {
    return {
      ...base,
      state: "degraded",
      healthy: false,
      storedQueuedLowerBound,
      message: `At least ${storedQueuedLowerBound.toLocaleString("en-US")} events are waiting on disk; recent MQTT data may be missing from history.`,
      action: "Inspect archiver ingest throughput, durable replay, and QuestDB writes.",
    };
  }

  return {
    ...base,
    state: "healthy",
    healthy: true,
    storedQueuedLowerBound,
  };
}
