export const ENTITY_BINDING_INVALIDATION_TOPIC = "uns-infra/entity-identity/binding-revision";
export const DEFAULT_ENTITY_SCOPE_KEY = "tenant:default";

export type EntityBindingInvalidation = {
  scopeKey: string;
  transitionId: string;
  bindingRevision: string;
  topicPaths: string[];
};

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function normalizeRevision(value: unknown): string | null {
  if (typeof value === "number") {
    return Number.isSafeInteger(value) && value > 0 ? String(value) : null;
  }
  if (typeof value !== "string" || !/^[1-9][0-9]*$/.test(value)) return null;
  return value;
}

function normalizeTopicPath(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const topic = value.trim();
  if (!topic || topic.startsWith("/") || topic.endsWith("/") || topic.includes("//") || /[+#]/.test(topic)) {
    return null;
  }
  return topic;
}

export function parseEntityBindingInvalidation(payload: unknown): EntityBindingInvalidation | null {
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) return null;
  const row = payload as Record<string, unknown>;
  if (row["schemaVersion"] !== 1 || row["eventType"] !== "entity-observation-binding-revision") {
    return null;
  }
  const scopeKey = typeof row["scopeKey"] === "string" ? row["scopeKey"].trim() : "";
  const transitionId = typeof row["transitionId"] === "string" ? row["transitionId"].trim() : "";
  const bindingRevision = normalizeRevision(row["bindingRevision"]);
  if (!scopeKey || !UUID_PATTERN.test(transitionId) || !bindingRevision || !Array.isArray(row["topicPaths"])) {
    return null;
  }
  const topicPaths = Array.from(new Set(row["topicPaths"].map(normalizeTopicPath)));
  if (topicPaths.some((topic) => topic === null) || topicPaths.length === 0) return null;
  return {
    scopeKey,
    transitionId: transitionId.toLowerCase(),
    bindingRevision,
    topicPaths: topicPaths as string[],
  };
}

export function isNewerBindingRevision(candidate: string, current: string | null): boolean {
  return current === null || BigInt(candidate) > BigInt(current);
}
