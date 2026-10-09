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
  storedProcessingFiles?: number;
  unresolvedProcessingFiles?: number;
  inspectionComplete?: boolean;
};

export function assessIngestHealth(
  storedQueuedLowerBound: number | null,
  checkedAt = new Date().toISOString(),
  inspection?: {
    scanComplete: boolean;
    processingFiles: number | null;
    unresolvedProcessingFiles?: number | null;
    capturedAt: string | null;
    lastError: string | null;
  },
): IngestHealth {
  const base = {
    id: "archiver-ingest" as const,
    label: "Archive ingest" as const,
    checkedAt,
    ...(inspection
      ? {
          inspectionComplete: inspection.scanComplete,
          ...(inspection.unresolvedProcessingFiles !== undefined &&
          inspection.unresolvedProcessingFiles !== null
            ? {
                unresolvedProcessingFiles: inspection.unresolvedProcessingFiles,
              }
            : {}),
          ...(inspection.processingFiles !== null
            ? { storedProcessingFiles: inspection.processingFiles }
            : {}),
        }
      : {}),
  };

  if (storedQueuedLowerBound === null) {
    return {
      ...base,
      state: "degraded",
      healthy: false,
      message:
        "The durable event backlog could not be inspected; archive freshness is unknown.",
      action:
        "Inspect the archiver event storage directory and its filesystem permissions.",
    };
  }

  if (storedQueuedLowerBound >= INGEST_BACKLOG_ALERT_EVENTS) {
    return {
      ...base,
      state: "degraded",
      healthy: false,
      storedQueuedLowerBound,
      message: inspection
        ? `The background inspection observed at least ${storedQueuedLowerBound.toLocaleString("en-US")} queued files; recent MQTT data may be missing from history.`
        : `At least ${storedQueuedLowerBound.toLocaleString("en-US")} events are waiting on disk; recent MQTT data may be missing from history.`,
      action:
        "Inspect archiver ingest throughput, durable replay, and QuestDB writes.",
    };
  }

  if (
    inspection &&
    (!inspection.scanComplete ||
      inspection.lastError ||
      !inspection.capturedAt ||
      !Number.isFinite(Date.parse(inspection.capturedAt)) ||
      Date.now() - Date.parse(inspection.capturedAt) > 60_000)
  ) {
    return {
      ...base,
      state: "degraded",
      healthy: false,
      storedQueuedLowerBound,
      message:
        "Durable backlog inspection is incomplete or stale; archive freshness is not yet confirmed.",
      action:
        "Inspect the background queue scan and processing recovery progress.",
    };
  }

  if (
    inspection &&
    (inspection.unresolvedProcessingFiles ?? inspection.processingFiles)
  ) {
    return {
      ...base,
      state: "degraded",
      healthy: false,
      storedQueuedLowerBound,
      message:
        "Processing files not tracked by this replay remain on disk; they may belong to another process or interrupted replay. Archive freshness needs inspection.",
      action:
        "Inspect replay activity and processing recovery; do not treat a zero queued count as an empty spool.",
    };
  }

  return {
    ...base,
    state: "healthy",
    healthy: true,
    storedQueuedLowerBound,
  };
}
