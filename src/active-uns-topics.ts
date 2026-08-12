import { request, gql } from "graphql-request";
import { UnsNode } from "./graphql/schema.js";
import { AuthClient, ConfigFile, logger, ServiceTokenProvider, type AccessTokenProvider } from "@uns-kit/core";
import { promises as fs } from "fs";
import path from "path";
import { CircuitBreaker, errorMessage, isRetryableNetworkError, withRetry } from "./resilience.js";

export interface UnsTopicMetadata {
  attribute?: string;
  objectId?: string;
  objectType?: string;
  asset?: string;
  dataGroup?: string;
}

const TOPIC_CACHE_FILE = path.resolve(process.cwd(), "active_topics_cache.json");
const GRAPHQL_CIRCUIT = new CircuitBreaker("uns-graphql", {
  failureThreshold: 5,
  openMs: 30000,
});

const hasFullTopic = (node: UnsNode): node is UnsNode & { fullTopic: string } => {
  return typeof node.fullTopic === "string";
};

const mapById = (nodes: UnsNode[]) => {
  const map = new Map<number, UnsNode>();
  for (const node of nodes) {
    if (typeof node.id === "number") {
      map.set(node.id, node);
    }
  }
  return map;
};

const resolveFromAncestors = (
  start: UnsNode | undefined,
  all: Map<number, UnsNode>,
  field: "asset" | "objectId" | "objectType",
) => {
  const seen = new Set<number>();
  let current = start;
  while (current) {
    const direct = (current as any)[field] as string | null | undefined;
    if (direct) return direct;

    if (field === "asset" && current.type === "Asset" && current.unsNode) return current.unsNode;
    if (field === "objectType" && current.type === "ObjectType" && current.unsNode) return current.unsNode;
    if (field === "objectId" && current.type === "ObjectId" && current.unsNode) return current.unsNode;

    const parentId = typeof current.parent === "number" ? current.parent : undefined;
    if (!parentId || seen.has(parentId)) break;
    seen.add(parentId);
    current = all.get(parentId);
  }
  return undefined;
};

const sanitizeTopic = (topic: string) => topic.endsWith("/") ? topic.slice(0, -1) : topic;

const deriveAttribute = (node: UnsNode, topic: string) => {
  if (node.unsNode) return node.unsNode;
  const parts = topic.split("/").filter(Boolean);
  return parts[parts.length - 1];
};

const readCache = async () => {
  try {
    const raw = await fs.readFile(TOPIC_CACHE_FILE, "utf-8");
    return JSON.parse(raw) as { topics: string[]; metaByTopic: Record<string, UnsTopicMetadata> };
  } catch {
    return null;
  }
};

const writeCache = async (payload: { topics: string[]; metaByTopic: Record<string, UnsTopicMetadata> }) => {
  try {
    await fs.writeFile(TOPIC_CACHE_FILE, JSON.stringify(payload), "utf-8");
  } catch {
    // best effort
  }
};

let legacyAuthClient: Promise<AuthClient | null> | undefined;

const legacyAuthFallback: AccessTokenProvider = {
  async getAccessToken(): Promise<string | undefined> {
    legacyAuthClient ??= AuthClient.create().catch(() => null);
    const client = await legacyAuthClient;
    return client?.getAccessToken();
  },
};

const buildAuthHeaders = async (config: { uns?: { token?: unknown } }): Promise<Record<string, string> | undefined> => {
  const provider = new ServiceTokenProvider({
    configToken: typeof config.uns?.token === "string" ? config.uns.token : undefined,
    fallback: legacyAuthFallback,
  });
  const token = await provider.getAccessToken();
  return token ? { Authorization: `Bearer ${token}` } : undefined;
};

export class ActiveUnsTopics {

  static async getActiveUnsTopics(): Promise<{ topics: string[]; metaByTopic: Record<string, UnsTopicMetadata> }> {
    const config = await ConfigFile.loadConfig();
    const document = gql`
    query GetUnsNodes {
      GetUnsNodes {
        id
        unsNode
        parent
        description
        type
        processName
        processVersion
        attributeTimestamp
        attributeNeedsPersistance
        attributeTags
        fullTopic
        apiDescription
        apiEndpoint
        apiMethod
        attributeType
        dataGroup
        objectType
        objectId
        asset
        apiProxyHost
        apiSwaggerEndpoint
        apiHost
      }
    }`;

    try {
      const query: any = await withRetry(
        "Load active UNS topics",
        async () => {
          const headers = await buildAuthHeaders(config);
          return await GRAPHQL_CIRCUIT.execute(
            async () => await request(config.uns.graphql, document, undefined, headers),
          );
        },
        {
          attempts: 4,
          baseDelayMs: 300,
          maxDelayMs: 3000,
          shouldRetry: (error) => {
            const status = (error as any)?.response?.status;
            if (status === 401 || status === 403) return true;
            return isRetryableNetworkError(error);
          },
          onRetry: ({ attempt, delayMs, error }) => {
            logger.warn(`GetUnsNodes attempt ${attempt} failed; retrying in ${delayMs}ms: ${errorMessage(error)}`);
          },
        },
      );
      const unsNodes: UnsNode[] | undefined = query.GetUnsNodes;
      if (!unsNodes) {
        const cached = await readCache();
        if (cached) return cached;
        throw new Error("Unable to load UNS topics from GraphQL or cache.");
      }

      const nodesById = mapById(unsNodes);

      const attributes = unsNodes
        .filter(item => item.type === "Attribute")
        .filter(hasFullTopic);

      const metaByTopic: Record<string, UnsTopicMetadata> = {};
      const topics = attributes.map(item => {
        const topic = sanitizeTopic(item.fullTopic);
        const attribute = deriveAttribute(item, topic);
        const asset = resolveFromAncestors(item, nodesById, "asset");
        const objectType = resolveFromAncestors(item, nodesById, "objectType");
        const objectId = resolveFromAncestors(item, nodesById, "objectId");
        const dataGroup = item.dataGroup ?? undefined;

        metaByTopic[topic] = { attribute, asset, objectType, objectId, dataGroup };
        return topic;
      });

      const result = { topics, metaByTopic };
      await writeCache(result);
      return result;
    } catch (err) {
      const cached = await readCache();
      if (cached) {
        logger.warn(`Using cached active topics due to GraphQL/auth error: ${errorMessage(err)}`);
        return cached;
      }
      throw err;
    }
  }
}
