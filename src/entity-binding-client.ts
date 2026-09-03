export type ArchiverEntityBindingStatus = "resolved" | "not-found" | "ambiguous";

export type ArchiverEntityBindingResolution = {
  topic: string;
  asOf: string | null;
  status: ArchiverEntityBindingStatus;
  stableEntityId: string | null;
  entityTypeKey: string | null;
  bindingKind: "asset-prefix" | "attribute-topic" | null;
  matchedPath: string | null;
  validFrom: string | null;
  validTo: string | null;
  timeBasis: string | null;
  sourceCount: number;
  revision: string | null;
  digest: string | null;
};

type AccessTokenProvider = {
  getAccessToken(): Promise<string | undefined>;
};

type CacheEntry = {
  resolution: ArchiverEntityBindingResolution;
  fetchedAt: number;
};

export type ArchiverEntityBindingClientOptions = {
  graphqlUrl: string;
  tokenProvider: AccessTokenProvider;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  cacheTtlMs?: number;
  staleIfErrorMs?: number;
  maxCacheEntries?: number;
  now?: () => number;
};

export type ArchiverEntityBindingResult = {
  resolution: ArchiverEntityBindingResolution | null;
  source: "cache" | "controller" | "stale-cache";
};

const RESOLVE_BINDING_QUERY = `
  query ResolveEntityObservationBindings($topics: [String!]!, $asOf: Timestamp) {
    ResolveEntityObservationBindings(topics: $topics, asOf: $asOf) {
      topic
      asOf
      status
      stableEntityId
      entityTypeKey
      bindingKind
      matchedPath
      validFrom
      validTo
      timeBasis
      sourceCount
      revision
      digest
    }
  }
`;

function normalizeTopic(value: string): string {
  const topic = value.trim().normalize("NFC").replace(/^\/+|\/+$/g, "");
  if (!topic || topic.includes("//") || /[+#]/.test(topic)) {
    throw new TypeError("topic must be one concrete non-empty UNS path");
  }
  return topic;
}

function normalizeAsOf(value?: string | Date): string | null {
  if (value === undefined) return null;
  const parsed = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(parsed.getTime())) throw new TypeError("asOf must be a valid timestamp");
  return parsed.toISOString();
}

function nullableString(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function parseResolution(value: unknown): ArchiverEntityBindingResolution | null {
  if (!value || typeof value !== "object") return null;
  const row = value as Record<string, unknown>;
  if (typeof row["topic"] !== "string") return null;
  if (row["status"] !== "resolved" && row["status"] !== "not-found" && row["status"] !== "ambiguous") return null;
  const sourceCount = Number(row["sourceCount"]);
  if (!Number.isSafeInteger(sourceCount) || sourceCount < 0) return null;
  const bindingKind = row["bindingKind"] === "asset-prefix" || row["bindingKind"] === "attribute-topic"
    ? row["bindingKind"]
    : null;
  return {
    topic: normalizeTopic(row["topic"]),
    asOf: nullableString(row["asOf"]),
    status: row["status"],
    stableEntityId: nullableString(row["stableEntityId"])?.trim().toLowerCase() ?? null,
    entityTypeKey: nullableString(row["entityTypeKey"]),
    bindingKind,
    matchedPath: nullableString(row["matchedPath"]),
    validFrom: nullableString(row["validFrom"]),
    validTo: nullableString(row["validTo"]),
    timeBasis: nullableString(row["timeBasis"]),
    sourceCount,
    revision: nullableString(row["revision"]),
    digest: nullableString(row["digest"]),
  };
}

export class ArchiverEntityBindingClient {
  private readonly fetchImpl: typeof fetch;
  private readonly timeoutMs: number;
  private readonly cacheTtlMs: number;
  private readonly staleIfErrorMs: number;
  private readonly maxCacheEntries: number;
  private readonly now: () => number;
  private readonly cache = new Map<string, CacheEntry>();
  private readonly cacheKeysByTopic = new Map<string, Set<string>>();

  constructor(private readonly options: ArchiverEntityBindingClientOptions) {
    if (!options.graphqlUrl.trim()) throw new TypeError("graphqlUrl is required");
    this.fetchImpl = options.fetchImpl ?? fetch;
    this.timeoutMs = options.timeoutMs ?? 5_000;
    this.cacheTtlMs = options.cacheTtlMs ?? 30_000;
    this.staleIfErrorMs = options.staleIfErrorMs ?? 300_000;
    this.maxCacheEntries = options.maxCacheEntries ?? 10_000;
    this.now = options.now ?? Date.now;
    if (
      this.timeoutMs <= 0
      || this.cacheTtlMs < 0
      || this.staleIfErrorMs < this.cacheTtlMs
      || !Number.isSafeInteger(this.maxCacheEntries)
      || this.maxCacheEntries < 1
    ) {
      throw new RangeError("identity binding client limits are invalid");
    }
  }

  async resolveTopic(topicValue: string, asOfValue?: string | Date): Promise<ArchiverEntityBindingResult> {
    const topic = normalizeTopic(topicValue);
    const asOf = normalizeAsOf(asOfValue);
    const key = `${asOf ?? "current"}\0${topic}`;
    const now = this.now();
    const cachedCandidate = this.cache.has(key)
      ? { key, entry: this.cache.get(key)! }
      : this.findReusableInterval(topic, asOf);
    const cached = cachedCandidate?.entry;
    if (cached && now - cached.fetchedAt <= this.cacheTtlMs) {
      this.touch(cachedCandidate!.key, cached);
      return { resolution: { ...cached.resolution, asOf }, source: "cache" };
    }

    try {
      const token = await this.options.tokenProvider.getAccessToken();
      if (!token) throw new Error("Controller identity binding request requires a service access token");
      const response = await this.fetchImpl(this.options.graphqlUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
        body: JSON.stringify({ query: RESOLVE_BINDING_QUERY, variables: { topics: [topic], asOf } }),
        signal: AbortSignal.timeout(this.timeoutMs),
      });
      const payload = await response.json() as {
        data?: { ResolveEntityObservationBindings?: unknown[] | null } | null;
        errors?: Array<{ message?: string }>;
      };
      if (!response.ok || payload.errors?.length) {
        throw new Error(payload.errors?.[0]?.message ?? `Controller identity binding request failed with HTTP ${response.status}`);
      }
      const rows = payload.data?.ResolveEntityObservationBindings;
      if (!Array.isArray(rows)) throw new Error("Controller entity binding response is missing data");
      const resolution = rows.map(parseResolution).find((row) => row?.topic === topic) ?? null;
      if (resolution) this.touch(key, { resolution, fetchedAt: now });
      return { resolution, source: "controller" };
    } catch (error) {
      if (cached && now - cached.fetchedAt <= this.staleIfErrorMs) {
        this.touch(cachedCandidate!.key, cached);
        return { resolution: { ...cached.resolution, asOf }, source: "stale-cache" };
      }
      throw error;
    }
  }

  invalidate(): void {
    this.cache.clear();
    this.cacheKeysByTopic.clear();
  }

  snapshot(): { entries: number; maxEntries: number } {
    return { entries: this.cache.size, maxEntries: this.maxCacheEntries };
  }

  private touch(key: string, entry: CacheEntry): void {
    this.cache.delete(key);
    this.cache.set(key, entry);
    const topicKeys = this.cacheKeysByTopic.get(entry.resolution.topic) ?? new Set<string>();
    topicKeys.add(key);
    this.cacheKeysByTopic.set(entry.resolution.topic, topicKeys);
    while (this.cache.size > this.maxCacheEntries) {
      const oldest = this.cache.keys().next().value as string | undefined;
      if (oldest === undefined) break;
      const removed = this.cache.get(oldest);
      this.cache.delete(oldest);
      if (removed) {
        const removedTopicKeys = this.cacheKeysByTopic.get(removed.resolution.topic);
        removedTopicKeys?.delete(oldest);
        if (removedTopicKeys?.size === 0) this.cacheKeysByTopic.delete(removed.resolution.topic);
      }
    }
  }

  private findReusableInterval(topic: string, asOf: string | null): { key: string; entry: CacheEntry } | undefined {
    if (asOf === null) return undefined;
    const instant = new Date(asOf).getTime();
    const keys = this.cacheKeysByTopic.get(topic);
    if (!keys) return undefined;
    for (const key of keys) {
      const entry = this.cache.get(key);
      const resolution = entry?.resolution;
      if (
        !entry
        || resolution?.status !== "resolved"
        || !resolution.validFrom
        || new Date(resolution.validFrom).getTime() > instant
        || (resolution.validTo !== null && new Date(resolution.validTo).getTime() <= instant)
      ) continue;
      return { key, entry };
    }
    return undefined;
  }
}
