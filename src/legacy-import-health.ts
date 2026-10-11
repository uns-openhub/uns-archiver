import type { LegacyImportManager } from "./legacy-import.js";

export type LegacyRecoveryState = "completed" | "running" | "paused" | "pending" | "blocked";

/** Recovery progress is separate from availability of the live MQTT writer. */
export function assessLegacyImportHealth(
  sources: ReturnType<LegacyImportManager["status"]>,
) {
  if (!sources.length) return null;
  const complete = (source: (typeof sources)[number]) =>
    source.loaded && !source.error && !source.inspection.error &&
    source.quarantinePresent !== true && !source.job?.lastError &&
    !(source.job?.quarantined ?? 0) && !(source.job?.deferred ?? 0) &&
    (source.job?.state === "completed" ||
      (!source.job && source.inspection.scanComplete &&
        source.inspection.events === 0 && source.inspection.processing === 0 &&
        source.inspection.other === 0));
  const unfinished = sources.filter((source) => !complete(source));
  const blocked = unfinished.some((source) =>
    source.error || source.inspection.error || source.quarantinePresent === true ||
    (source.job?.lastError && !["restart-review-required", "shutdown-review-required"].includes(source.job.lastError)) ||
    (source.job?.quarantined ?? 0) > 0 ||
    (source.job?.deferred ?? 0) > 0 ||
    source.job?.state === "blocked" || source.job?.state === "cancelled");
  const recoveryState: LegacyRecoveryState = !unfinished.length ? "completed" : blocked ? "blocked" :
    unfinished.some((source) => !source.loaded || !source.job || source.job.state === "planned") ? "pending" :
    unfinished.some((source) => source.job?.state === "paused") ? "paused" : "running";
  const messages = {
    running: ["Legacy archive recovery is running. Historical recovery is not yet complete.",
      "Monitor import progress and live ingest. Pause import if write errors or live queue pressure increase."],
    paused: ["Legacy archive recovery is paused. Live MQTT archiving can continue independently.",
      "Review the import and resume it after confirming the old writer remains stopped."],
    pending: ["Legacy archive recovery has not yet been confirmed complete.",
      "Inspect legacy import status. Confirm the old writer has exited before starting recovery."],
    blocked: ["Legacy archive recovery requires review. Live writer health is reported separately.",
      "Inspect import errors, source ownership and preserved files before resuming recovery."],
  } as const;
  const healthy = recoveryState === "completed";
  return {
    id: "archiver-legacy-import",
    label: "Legacy archive recovery",
    state: healthy ? "healthy" : "degraded",
    healthy,
    recoveryState,
    checkedAt: new Date().toISOString(),
    ...(recoveryState !== "completed" ? { message: messages[recoveryState][0], action: messages[recoveryState][1] } : {}),
  };
}
