export type EventDeduplicationDisposition =
  "process" | "skip-confirmed" | "defer-inflight";

/**
 * A completed event may be discarded, but a stored event that merely overlaps
 * an active live write must remain durable until that write has resolved.
 */
export const resolveEventDeduplicationDisposition = (
  processed: boolean,
  inflight: boolean,
  fromStorage: boolean,
): EventDeduplicationDisposition => {
  if (processed) return "skip-confirmed";
  if (inflight && fromStorage) return "defer-inflight";
  if (inflight) return "skip-confirmed";
  return "process";
};
