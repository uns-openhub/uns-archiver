export type InactiveTopicBufferEntry = {
  receivedAt: number;
};

/**
 * Removes packets that were never admitted into the active UNS topic registry.
 *
 * The buffer bridges short controller/metadata races only. It is deliberately
 * not a durable queue: persisting an ineligible packet would make a broad MQTT
 * filter retain unrelated system telemetry forever.
 */
export const discardExpiredOrOverflowInactiveEvents = <
  T extends InactiveTopicBufferEntry,
>(
  eventsByTopic: Map<string, T[]>,
  maxEvents: number,
  maxAgeMs: number,
  now: number,
): T[] => {
  const discarded: T[] = [];
  let total = 0;

  for (const [topic, events] of eventsByTopic) {
    while (events.length > 0 && now - events[0]!.receivedAt > maxAgeMs) {
      discarded.push(events.shift()!);
    }
    if (events.length === 0) {
      eventsByTopic.delete(topic);
      continue;
    }
    total += events.length;
  }

  while (total > maxEvents) {
    let oldestTopic: string | undefined;
    let oldestAt = Infinity;
    for (const [topic, events] of eventsByTopic) {
      const oldest = events[0];
      if (oldest && oldest.receivedAt < oldestAt) {
        oldestAt = oldest.receivedAt;
        oldestTopic = topic;
      }
    }
    if (!oldestTopic) break;

    const events = eventsByTopic.get(oldestTopic)!;
    discarded.push(events.shift()!);
    total -= 1;
    if (events.length === 0) eventsByTopic.delete(oldestTopic);
  }

  return discarded;
};

/**
 * Older releases persisted packets that had already exceeded the inactive
 * buffer. They cannot become eligible merely by replaying them, so safely
 * acknowledge them while draining the legacy spool.
 */
export const isInactiveBufferSpill = (event: unknown): boolean => {
  if (!event || typeof event !== "object") return false;
  const reason = (event as { bufferReason?: unknown }).bufferReason;
  return reason === "inactive_expired" || reason === "inactive_overflow";
};
