export interface SubscriptionDelta {
  subscribe: string[];
  unsubscribe: string[];
  next: string[];
}

export function canonicalizeTopics(topics: readonly string[]): string[] {
  return Array.from(
    new Set(
      topics
        .map((topic) => (typeof topic === "string" && topic.endsWith("/") ? topic.slice(0, -1) : topic))
        .filter((topic): topic is string => typeof topic === "string" && topic.length > 0),
    ),
  ).sort();
}

export function subscriptionDelta(
  currentFilters: Iterable<string>,
  desiredFilters: readonly string[],
): SubscriptionDelta {
  const current = canonicalizeTopics(Array.from(currentFilters));
  const next = canonicalizeTopics(desiredFilters);
  const currentSet = new Set(current);
  const nextSet = new Set(next);

  return {
    subscribe: next.filter((filter) => !currentSet.has(filter)),
    unsubscribe: current.filter((filter) => !nextSet.has(filter)),
    next,
  };
}
