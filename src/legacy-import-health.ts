import type { LegacyImportManager } from "./legacy-import.js";

/** A healthy new live spool must not hide configured, unfinished legacy recovery. */
export function assessLegacyImportHealth(
  sources: ReturnType<LegacyImportManager["status"]>,
) {
  if (!sources.length) return null;
  const healthy = sources.every(
    (source) =>
      source.loaded &&
      !source.error &&
      (source.job?.state === "completed" ||
        (!source.job &&
          source.inspection.scanComplete &&
          !source.inspection.error &&
          source.inspection.events === 0 &&
          source.inspection.processing === 0 &&
          source.inspection.other === 0)),
  );
  return {
    id: "archiver-legacy-import",
    label: "Legacy archive recovery",
    state: healthy ? "healthy" : "degraded",
    healthy,
    checkedAt: new Date().toISOString(),
    ...(!healthy
      ? {
          message:
            "Configured legacy archive recovery is pending or requires review.",
          action:
            "Inspect legacy import status. Confirm the old writer has exited before starting or resuming recovery.",
        }
      : {}),
  };
}
