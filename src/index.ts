import {
  UnsProxyProcess,
  ConfigFile,
  logger,
  mqttChannelParameters,
  registerService,
  resolveMqttChannel,
  ServiceTokenProvider,
  UnsClient,
  type IApiProxyOptions,
} from "@uns-kit/core";
import UnsMqttProxy from "@uns-kit/core/uns-mqtt/uns-mqtt-proxy.js";
import { Sender } from "@questdb/nodejs-client";
import { createHash } from "crypto";
import { promises as fs } from "fs";
import * as path from "path";
import { buildServiceApiInteractions, type UnsProxyProcessWithApi } from "@uns-kit/api";

import { ActiveUnsTopics, UnsTopicMetadata } from "./active-uns-topics.js";
import { TopicMatcher } from "./topic-matcher.js";
import { UnsPacket } from "@uns-kit/core/uns/uns-packet.js";

import { QuestDBWriter, type QuestDbDependencyHealth } from "./writers/questDbWriter.js";
import { NonRetryableError } from "./errors.js";
import { buildQuestDbTableName } from "./questdb-table-name.js";
import { CircuitBreaker, errorMessage, isRetryableNetworkError, withRetry } from "./resilience.js";
let pkgInfo: { name: string; version: string } | null = null;


const EVENT_STORAGE_DIR = "./event_storage";
const EVENT_FILE_EXTENSION = ".event";
const EVENT_PROCESSING_EXTENSION = ".processing";
const FAILED_EVENT_STORAGE_DIR = path.join(EVENT_STORAGE_DIR, "failed");
const TOPICS_REFRESH_INTERVAL = 30000; // 30 seconds
const CONFIG_REFRESH_INTERVAL = 45000; // 30 seconds
const STORED_EVENTS_PROCESS_INTERVAL = 60000; // 1 minute
const MAPPING_PUBLISH_INTERVAL = 60000; // 1 minute
const PROCESSED_EVENTS_CACHE_SIZE = 10000; // Adjust as needed
const API_HEALTHCHECK_INTERVAL = 60000; // 1 minute
const QUESTDB_HEALTHCHECK_INTERVAL = 30000; // 30 seconds

function sanitizeTopicName(topic: string): string {
  return typeof topic === "string" && topic.endsWith("/") ? topic.slice(0, -1) : topic;
}

const sanitizeReason = (value: string): string =>
  value
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 48) || "unknown";

function filterSubsumes(broaderFilter: string, narrowerFilter: string): boolean {
  const broader = broaderFilter.split("/");
  const narrower = narrowerFilter.split("/");

  const canMatchEmptySuffix = (segments: string[], index: number): boolean =>
    index >= segments.length || (segments[index] === "#" && index === segments.length - 1);

  const visit = (index: number): boolean => {
    if (index >= broader.length && index >= narrower.length) return true;
    if (index >= broader.length) return false;
    if (index >= narrower.length) {
      return canMatchEmptySuffix(broader, index);
    }

    const broaderSegment = broader[index];
    const narrowerSegment = narrower[index];

    if (broaderSegment === "#") return true;
    if (narrowerSegment === "#") {
      return false;
    }

    if (broaderSegment === "+") {
      return narrowerSegment !== "#" && visit(index + 1);
    }

    if (narrowerSegment === "+") {
      return false;
    }

    if (broaderSegment !== narrowerSegment) {
      return false;
    }

    return visit(index + 1);
  };

  return visit(0);
}

function minimizeSubscriptionFilters(filters: string[]): string[] {
  const uniqueFilters: string[] = [];
  for (const filter of filters) {
    if (!filter) continue;
    if (uniqueFilters.includes(filter)) continue;
    uniqueFilters.push(filter);
  }

  return uniqueFilters.filter(
    (candidate, index) =>
      !uniqueFilters.some(
        (other, otherIndex) => otherIndex !== index && filterSubsumes(other, candidate),
      ),
  );
}

type IngestMode = "append" | "dedup" | "window_replace" | undefined;
type DataGroupOverride = {
  name: string;
  ingestMode?: IngestMode;
  ingestModeData?: IngestMode;
  ingestModeTable?: IngestMode;
};

function resolveIngestMode(storage: any, unsPacket: any): IngestMode {
  const base: IngestMode = storage?.ingestMode;
  const isData = !!unsPacket?.message?.data;
  const isTable = !!unsPacket?.message?.table;
  const dataGroup: string | undefined =
    (isData && unsPacket?.message?.data?.dataGroup) || (isTable && unsPacket?.message?.table?.dataGroup) || undefined;
  const groupOverride: DataGroupOverride | undefined = Array.isArray(storage?.dataGroups)
    ? storage.dataGroups.find((g: any) => g?.name === dataGroup)
    : undefined;

  const modeFromGroup = groupOverride?.ingestMode;
  const modeData = isData ? (groupOverride?.ingestModeData ?? storage?.ingestModeData) : undefined;
  const modeTable = isTable ? (groupOverride?.ingestModeTable ?? storage?.ingestModeTable) : undefined;

  return modeData ?? modeTable ?? modeFromGroup ?? base;
}

let config = await ConfigFile.loadConfig();
let mqttInput: UnsMqttProxy | undefined;
let lastMappingHash: string | null = null;
let apiProxy: any | undefined;
let apiRestartScheduled = false;
let ingestionPaused = false;
let lastTopicsRefreshAt: number | null = null;
let storedEventsRunInProgress = false;
let latestQuestDbHealth: QuestDbDependencyHealth | null = null;
let serviceMetadataPublisher: UnsProxyProcessWithApi | undefined;

const mqttPublishCircuit = new CircuitBreaker("mqtt-mapping-publish", {
  failureThreshold: 5,
  openMs: 15000,
});

type ArchiverRuntimeConfig = {
  inactiveBufferMax?: number;
  inactiveBufferMaxAgeMs?: number;
  traceIngest?: boolean;
};

const resolveTraceIngestFromEnv = (): boolean =>
  process.env.UNS_ARCHIVER_TRACE === "1" || process.env.UNS_ARCHIVER_TRACE_INGEST === "1";

let traceIngestEnabled = resolveTraceIngestFromEnv();
let inactiveBufferMaxEvents = 2000;
let inactiveBufferMaxAgeMs = 5 * 60 * 1000;

const refreshArchiverRuntimeSettings = () => {
  const cfg: ArchiverRuntimeConfig | undefined = (config as any)?.archiver;
  traceIngestEnabled = cfg?.traceIngest ?? resolveTraceIngestFromEnv();
  inactiveBufferMaxEvents =
    typeof cfg?.inactiveBufferMax === "number"
      ? cfg.inactiveBufferMax
      : Number(process.env.UNS_ARCHIVER_INACTIVE_BUFFER_MAX ?? 2000);
  inactiveBufferMaxAgeMs =
    typeof cfg?.inactiveBufferMaxAgeMs === "number"
      ? cfg.inactiveBufferMaxAgeMs
      : Number(process.env.UNS_ARCHIVER_INACTIVE_BUFFER_MAX_AGE_MS ?? 5 * 60 * 1000);
};

refreshArchiverRuntimeSettings();
logger.info(
  `Archiver ingest trace is ${traceIngestEnabled ? "ENABLED" : "DISABLED"} (set UNS_ARCHIVER_TRACE=1 to enable).`,
);
logger.info(
  `Archiver configured topic filters: ${(config.questdb?.dataStorage ?? []).map((s: any) => s?.topic).filter(Boolean).join(", ") || "(none)"}`,
);
const normalizeBasePrefix = (value: string | undefined | null): string => {
  if (!value) return "";
  const trimmed = value.trim();
  if (!trimmed) return "";
  const withLeading = trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
  return withLeading.replace(/\/+$/, "");
};

const CONTROL_TOPIC: string = "system/";
const CONTROL_ASSET: string = "archiver";
const CONTROL_OBJECT_TYPE: string = "service";
const CONTROL_OBJECT_ID: string = config.uns?.processName ?? "uns-archiver";

// Initialize QuestDB ILP sender for data ingestion
const questDbOutput = await Sender.fromConfig(config.questdb.configurationString);
const questDbWriter = new QuestDBWriter(questDbOutput, config.questdb.configurationString);

async function refreshQuestDbHealth(): Promise<QuestDbDependencyHealth> {
  latestQuestDbHealth = await questDbWriter.checkHealth();
  return latestQuestDbHealth;
}

async function publishArchiverServiceMetadata() {
  if (!serviceMetadataPublisher) return;
  const health = latestQuestDbHealth ?? (await refreshQuestDbHealth());
  await serviceMetadataPublisher.publishServiceMetadata({
    serviceId: "uns-archiver",
    kind: "core",
    label: "UNS Archiver",
    description: "Persists UNS time-series data into QuestDB and publishes QuestDB table mappings.",
    capabilities: ["history", "questdb-mapping", "graph-data"],
    extra: {
      dependencies: [health],
      questdbHealth: health,
    },
  });
}

async function refreshAndPublishQuestDbHealth() {
  const previousHealthy = latestQuestDbHealth?.healthy;
  const health = await refreshQuestDbHealth();
  await publishArchiverServiceMetadata();
  if (previousHealthy !== health.healthy) {
    const suffix = health.message ? ` (${health.message})` : "";
    logger.info(`QuestDB dependency health is ${health.state}${suffix}.`);
  }
}

// In-memory cache to keep track of processed event IDs
const processedEventIds = new Set<string>();
const inflightEventIds = new Set<string>();

// Active-topics gating + buffer:
// - The archiver can *see* broad topic filters (e.g. enterprise/#), but should only *persist*
//   when the full topic is present in activeTopics (controller/GraphQL registry).
// - To avoid losing early messages while the controller hasn't registered the topic yet,
//   we keep a small in-memory buffer and flush it once the topic becomes active.
const activeTopicSet = new Set<string>();
const rebuildActiveTopicSet = () => {
  activeTopicSet.clear();
  for (const t of activeTopics ?? []) {
    const normalized = sanitizeTopicName(String(t ?? ""));
    if (normalized) activeTopicSet.add(normalized);
  }
};

type BufferedMqttEvent = { mqttEvent: { topic: any; message: any }; eventId: string; receivedAt: number };
const inactiveBufferByTopic = new Map<string, BufferedMqttEvent[]>();
const inactiveBufferEventIds = new Set<string>();

const bufferInactiveEvent = async (
  topic: string,
  mqttEvent: { topic: any; message: any },
  eventId: string,
): Promise<void> => {
  if (inactiveBufferEventIds.has(eventId)) return;

  const now = Date.now();
  const normalizedTopic = sanitizeTopicName(String(topic ?? ""));
  if (!normalizedTopic) return;

  const arr = inactiveBufferByTopic.get(normalizedTopic) ?? [];
  arr.push({ mqttEvent, eventId, receivedAt: now });
  inactiveBufferByTopic.set(normalizedTopic, arr);
  inactiveBufferEventIds.add(eventId);

  // Cleanup expired / over-capacity events (spill to local storage to avoid losing data).
  const spill = async (ev: BufferedMqttEvent, reason: string) => {
    inactiveBufferEventIds.delete(ev.eventId);
    try {
      await saveEventToFile({ ...ev.mqttEvent, bufferReason: reason, bufferedAt: new Date(ev.receivedAt).toISOString() }, ev.eventId);
    } catch {
      // best-effort
    }
  };

  const totalCount = () => {
    let total = 0;
    for (const list of inactiveBufferByTopic.values()) total += list.length;
    return total;
  };

  if (traceIngestEnabled) {
    logger.info(
      `[trace][buffer] queued eventId=${eventId} topic='${normalizedTopic}' bufferedTopic=${arr.length} bufferedTotal=${totalCount()}`,
    );
  }

  // Expire per-topic oldest first
  while (arr.length > 0 && now - arr[0]!.receivedAt > inactiveBufferMaxAgeMs) {
    const ev = arr.shift()!;
    await spill(ev, "inactive_expired");
  }

  // Global cap: remove oldest across topics (simple scan; bounded by cap)
  while (totalCount() > inactiveBufferMaxEvents) {
    let oldestTopic: string | null = null;
    let oldestIndex = 0;
    let oldestAt = Infinity;
    for (const [t, list] of inactiveBufferByTopic.entries()) {
      if (list.length === 0) continue;
      const at = list[0]!.receivedAt;
      if (at < oldestAt) {
        oldestAt = at;
        oldestTopic = t;
        oldestIndex = 0;
      }
    }
    if (!oldestTopic) break;
    const list = inactiveBufferByTopic.get(oldestTopic);
    if (!list || list.length === 0) {
      inactiveBufferByTopic.delete(oldestTopic);
      continue;
    }
    const ev = list.splice(oldestIndex, 1)[0]!;
    if (list.length === 0) inactiveBufferByTopic.delete(oldestTopic);
    await spill(ev, "inactive_overflow");
  }
};

const flushInactiveBuffer = async (): Promise<void> => {
  const topicsToFlush = Array.from(inactiveBufferByTopic.keys()).filter((t) => activeTopicSet.has(t));
  if (topicsToFlush.length === 0) return;

  if (traceIngestEnabled) {
    const total = topicsToFlush.reduce((acc, t) => acc + (inactiveBufferByTopic.get(t)?.length ?? 0), 0);
    logger.info(`[trace][buffer] flush_start topics=${topicsToFlush.length} events=${total}`);
  }

  for (const t of topicsToFlush) {
    const buffered = inactiveBufferByTopic.get(t);
    if (!buffered || buffered.length === 0) {
      inactiveBufferByTopic.delete(t);
      continue;
    }
    inactiveBufferByTopic.delete(t);
    for (const ev of buffered) {
      inactiveBufferEventIds.delete(ev.eventId);
      try {
        await processEvent(ev.mqttEvent, false, { bypassActiveTopicCheck: true });
      } catch (err) {
        // best-effort: spill to disk so we don't lose it
        try {
          await saveEventToFile({ ...ev.mqttEvent, bufferReason: "flush_error", bufferedAt: new Date(ev.receivedAt).toISOString() }, ev.eventId);
        } catch {
          // ignore
        }
        if (traceIngestEnabled) {
          logger.info(
            `[trace][buffer] flush_error eventId=${ev.eventId} topic='${String(t)}' err=${err instanceof Error ? err.message : String(err)}`,
          );
        }
      }
    }
  }
};
type ObservedDataGroups = { data: Set<string | null>; table: Set<string | null> };
const observedDataGroupsByTopic = new Map<string, ObservedDataGroups>();

const recordObservedDataGroup = (
  topic: string,
  kind: "data" | "table",
  dataGroup: string | null | undefined,
): boolean => {
  const normalizedTopic = sanitizeTopicName(topic ?? "");
  if (!normalizedTopic) return false;
  let entry = observedDataGroupsByTopic.get(normalizedTopic);
  if (!entry) {
    entry = { data: new Set(), table: new Set() };
    observedDataGroupsByTopic.set(normalizedTopic, entry);
  }
  const set = kind === "data" ? entry.data : entry.table;
  const value = dataGroup ?? null;
  if (set.has(value)) return false;
  set.add(value);
  return true;
};

const getObservedDataGroups = (topic: string, suffix: "_data" | "_table"): Array<string | null> => {
  const normalizedTopic = sanitizeTopicName(topic);
  const observed = observedDataGroupsByTopic.get(normalizedTopic);
  const set = suffix === "_data" ? observed?.data : observed?.table;
  if (set && set.size > 0) return Array.from(set);
  return [];
};

// Periodically clean up the processed events cache to prevent it from growing indefinitely
setInterval(() => {
  processedEventIds.clear();
  logger.info("Cleared processed events cache.");
}, 3600000); // Every hour

await processStoredEvents();

let activeTopics: string[] = [];
let topicMetadata: Record<string, UnsTopicMetadata> = {};
try {
  const active = await ActiveUnsTopics.getActiveUnsTopics();
  activeTopics = active.topics;
  topicMetadata = active.metaByTopic;
  rebuildActiveTopicSet();
  await subscribeToTopics(activeTopics);
  await setupApiProxy();
} catch (error) {
  const reason = error instanceof Error ? error : new Error(String(error));
  logger.error(`Failed to refresh active topics on startup: ${reason.message}`);
}

if (isControllerManagedRuntime()) {
  if (!apiProxy) {
    throw new Error("UNS Archiver did not start its API runtime, so it cannot register with the controller.");
  }
  await registerArchiverService();
}

setInterval(async () => {
  await refreshActiveTopics();
}, TOPICS_REFRESH_INTERVAL);

setInterval(async () => {
  await processStoredEvents();
}, STORED_EVENTS_PROCESS_INTERVAL);

setInterval(async () => {
  try {
    const { dataStorageChanged } = await reloadConfig();
    if (dataStorageChanged) {
      if (mqttInput) {
        const topicsToUnsubscribe = activeTopics.filter(Boolean);
        if (topicsToUnsubscribe.length > 0) {
          await mqttInput.unsubscribeAsync(topicsToUnsubscribe);
        }
      }
      await subscribeToTopics(activeTopics);
      await publishQuestDbMapping();
    }
  } catch (error) {
    const reason = error instanceof Error ? error : new Error(String(error));
    logger.error(`Failed to refresh active config: ${reason.message}`);
  }
}, CONFIG_REFRESH_INTERVAL);

setInterval(async () => {
  await publishQuestDbMapping();
}, MAPPING_PUBLISH_INTERVAL);

setInterval(async () => {
  try {
    await refreshAndPublishQuestDbHealth();
  } catch (error) {
    logger.warn(`Failed to publish QuestDB dependency health: ${errorMessage(error)}`);
  }
}, QUESTDB_HEALTHCHECK_INTERVAL);

setInterval(async () => {
  if (!apiProxy) {
    await setupApiProxy();
  }
}, API_HEALTHCHECK_INTERVAL);

async function getPackageInfo(): Promise<{ name: string; version: string }> {
  if (pkgInfo) return pkgInfo;
  const packageJsonPath = await findNearestPackageJson();
  if (!packageJsonPath) {
    pkgInfo = { name: "uns-archiver", version: "0.0.0" };
    return pkgInfo;
  }
  try {
    const raw = await fs.readFile(packageJsonPath, "utf8");
    const parsed = JSON.parse(raw);
    pkgInfo = { name: parsed.name ?? "uns-archiver", version: parsed.version ?? "0.0.0" };
  } catch {
    pkgInfo = { name: "uns-archiver", version: "0.0.0" };
  }
  return pkgInfo;
}

function isControllerManagedRuntime(): boolean {
  return Boolean(process.env.RTT_NODE?.trim() && process.env.RTT_INSTANCE_ID?.trim());
}

async function registerArchiverService(): Promise<void> {
  if (!isControllerManagedRuntime()) return;

  const controllerRestUrl = typeof config.uns?.rest === "string" ? config.uns.rest.trim() : "";
  if (!controllerRestUrl) {
    throw new Error("Controller-managed UNS Archiver requires config.uns.rest for service registration.");
  }

  const packageInfo = await getPackageInfo();
  const registration = await registerService({
    client: new UnsClient(controllerRestUrl, {
      tokenProvider: new ServiceTokenProvider({
        configToken: typeof config.uns?.token === "string" ? config.uns.token : undefined,
      }),
    }),
    service: {
      id: "uns-archiver",
      version: packageInfo.version,
      capabilities: ["history", "questdb-mapping", "graph-data"],
      healthContract: "service-metadata-v1",
      processName: config.uns.processName,
    },
  });
  if (registration) {
    logger.info(`Registered controller-managed service ${registration.service.rttNode}/${registration.service.instanceId}.`);
  }
}

async function reloadConfig(): Promise<{ dataStorageChanged: boolean }> {
  const previous = config;
  ConfigFile.clearCache();
  const updated = await ConfigFile.loadConfig();
  config = updated;
  refreshArchiverRuntimeSettings();
  const previousDataStorage = JSON.stringify(previous?.questdb?.dataStorage ?? []);
  const updatedDataStorage = JSON.stringify(updated.questdb?.dataStorage ?? []);
  return { dataStorageChanged: previousDataStorage !== updatedDataStorage };
}

async function findNearestPackageJson(): Promise<string | null> {
  let dir = process.cwd();
  const checked = new Set<string>();
  while (true) {
    const candidate = path.join(dir, "package.json");
    if (!checked.has(candidate)) {
      try {
        await fs.access(candidate);
        return candidate;
      } catch {
        checked.add(candidate);
      }
    }
    const parent = path.dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }
  return null;
}

/**
 * Publish QuestDB mapping info so the controller can expose APIs.
 */
async function publishQuestDbMapping() {
  if (!mqttInput) return;
  try {
    const pkg = await getPackageInfo();
    const tableSuffixes = ["_data", "_table"] as const;
    const mappingsFromTopics = activeTopics.flatMap((topic) => {
      const tablePrefix = TopicMatcher.findTable(config, topic);
      if (!tablePrefix) return [];
      return tableSuffixes.flatMap((suffix) =>
        getObservedDataGroups(topic, suffix).map((dataGroup) => ({
          tableName: buildQuestDbTableName(tablePrefix, dataGroup, suffix),
          tablePrefix,
          dataGroup,
          suffix,
          topic,
        }))
      );
    });
    const mappings = mappingsFromTopics;
    const payload = {
      package: pkg.name,
      version: pkg.version,
      processName: config.uns?.processName ?? "uns-archiver",
      questdb: {
        configurationString: config.questdb?.configurationString,
      },
      mappings,
      updatedAt: new Date().toISOString(),
    };
    const hash = createHash("sha256").update(JSON.stringify(payload)).digest("hex");
    if (hash === lastMappingHash) return;
    lastMappingHash = hash;
    const versionSegment = pkg.version.replace(/\./g, "-");
    const topic = `uns-infra/${pkg.name}/${versionSegment}/${config.uns?.processName ?? "uns-archiver"}/questdb-mapping`;
    const message = JSON.stringify(payload);
    await withRetry(
      "Publish QuestDB mapping",
      async () =>
        await mqttPublishCircuit.execute(async () => {
          await mqttInput!.publishMessage(topic, message);
        }),
      {
        attempts: 4,
        baseDelayMs: 250,
        maxDelayMs: 2500,
        shouldRetry: isRetryableNetworkError,
        onRetry: ({ attempt, delayMs, error }) => {
          logger.warn(`QuestDB mapping publish retry (attempt ${attempt}) in ${delayMs}ms: ${errorMessage(error)}`);
        },
      },
    );
  } catch (err) {
    logger.error(`Failed to publish QuestDB mapping: ${err instanceof Error ? err.message : String(err)}`);
  }
}

async function setupApiProxy() {
  if (apiProxy) return;
  const infraChannel = resolveMqttChannel(config.infra);

  const apiOptions = buildApiOptions();
  try {
    const processWithApi = new UnsProxyProcess(infraChannel.host, {
      processName: config.uns.processName,
      ...mqttChannelParameters(infraChannel),
    }) as UnsProxyProcessWithApi;
    serviceMetadataPublisher = processWithApi;

    apiProxy = await processWithApi.createApiProxy("unsArchiverApi", apiOptions);
    const apiBasePrefix =
      normalizeBasePrefix(process.env.UNS_API_BASE_PATH as string | undefined) ||
      normalizeBasePrefix(config.uns?.processName);
    const swaggerBasePrefix =
      normalizeBasePrefix(process.env.UNS_SWAGGER_BASE_PATH as string | undefined) || apiBasePrefix;

    // Expose API and swagger under a custom base prefix to avoid clashing with the controller's /api
    const router = (apiProxy as any)?.app?.router;
    const expressApp = (apiProxy as any)?.app?.expressApplication;
    const originalRegister = apiProxy.registerApiEndpoint?.bind(apiProxy);
    const processName = (apiProxy as any)?.processName ?? config.uns.processName;
    const instanceName = (apiProxy as any)?.instanceName ?? "unsArchiverApi";

    if (expressApp && router && apiBasePrefix) {
      expressApp.use(`${apiBasePrefix}/api`, router);
    }
    if (expressApp && swaggerBasePrefix && typeof (apiProxy as any)?.app?.getSwaggerSpec === "function") {
      const swaggerPath = `/${processName}/${instanceName}/swagger.json`;
      expressApp.get(`${swaggerBasePrefix}${swaggerPath}`.replace(/\/{2,}/g, "/"), (_req: any, res: any) =>
        res.json((apiProxy as any).app.getSwaggerSpec())
      );
      const spec = (apiProxy as any).app.swaggerSpec;
      if (spec) {
        spec.servers = [{ url: swaggerBasePrefix || "/" }];
      }
    }
    if (originalRegister) {
      apiProxy.registerApiEndpoint = (apiObject: any) => {
        const rebasedApiEndpoint = apiBasePrefix
          ? `${apiBasePrefix}${apiObject.apiEndpoint}`.replace(/\/{2,}/g, "/")
          : apiObject.apiEndpoint;
        const rebasedSwagger = swaggerBasePrefix
          ? `${swaggerBasePrefix}${apiObject.apiSwaggerEndpoint}`.replace(/\/{2,}/g, "/")
          : apiObject.apiSwaggerEndpoint;
        return originalRegister({
          ...apiObject,
          apiEndpoint: rebasedApiEndpoint,
          apiSwaggerEndpoint: rebasedSwagger,
        });
      };
    }
    const serviceApiInteractions = buildServiceApiInteractions(processName, {
      control: {
        topic: CONTROL_TOPIC,
        asset: CONTROL_ASSET,
        objectType: CONTROL_OBJECT_TYPE,
        objectId: CONTROL_OBJECT_ID,
        attribute: "control",
        method: "GET",
        description: "Control archiver (action=pause|resume|status). Default is status.",
        tags: ["archiver", "control"],
        queryParams: [
          {
            name: "action",
            in: "query",
            type: "string",
            required: false,
            description: "pause | resume | status",
            enumValues: ["pause", "resume", "status"],
          },
        ],
        response: {
          statusCode: "200",
          description: "Current or updated archiver control state.",
          contentType: "application/json",
        },
        handler: () => undefined,
      },
      topics: {
        topic: CONTROL_TOPIC,
        asset: CONTROL_ASSET,
        objectType: CONTROL_OBJECT_TYPE,
        objectId: CONTROL_OBJECT_ID,
        attribute: "topics",
        method: "GET",
        description: "Report current subscriptions and queues.",
        tags: ["archiver", "topics"],
        response: {
          statusCode: "200",
          description: "Current archiver topic subscriptions and queue state.",
          contentType: "application/json",
        },
        handler: () => undefined,
      },
    });
    await apiProxy.get(
      CONTROL_TOPIC,
      CONTROL_ASSET,
      CONTROL_OBJECT_TYPE,
      CONTROL_OBJECT_ID,
      "control",
      serviceApiInteractions.control.options
    );
    await apiProxy.get(
      CONTROL_TOPIC,
      CONTROL_ASSET,
      CONTROL_OBJECT_TYPE,
      CONTROL_OBJECT_ID,
      "topics",
      serviceApiInteractions.topics.options
    );

    await publishArchiverServiceMetadata();

    apiProxy.event.on("apiGetEvent", handleApiGetEvent);
    apiProxy.event.on("error", (err: any) => {
      const reason = err instanceof Error ? err.message : String(err);
      logger.error(`API proxy error: ${reason}, scheduling restart...`);
      scheduleApiProxyRestart();
    });
    logger.info("Archiver control API ready (pause/resume/status).");
  } catch (err) {
    const reason = err instanceof Error ? err.message : String(err);
    logger.error(`Failed to start archiver control API: ${reason}`);
  }
}

function scheduleApiProxyRestart() {
  if (apiRestartScheduled) return;
  apiRestartScheduled = true;
  setTimeout(async () => {
    apiRestartScheduled = false;
    apiProxy = undefined;
    await setupApiProxy();
  }, 5000);
}

function buildApiOptions(): IApiProxyOptions {
  if (config.uns?.jwksWellKnownUrl) {
    return {
      jwks: {
        wellKnownJwksUrl: config.uns.jwksWellKnownUrl,
        ...(config.uns.kidWellKnownUrl ? { activeKidUrl: config.uns.kidWellKnownUrl } : {}),
      },
    };
  }
  const jwtSecret = process.env.UNS_API_JWT_SECRET?.trim();
  if (!jwtSecret) {
    throw new Error(
      "API authentication is not configured. Set uns.jwksWellKnownUrl or UNS_API_JWT_SECRET.",
    );
  }
  return {
    jwtSecret,
  };
}

async function handleApiGetEvent(event: any) {
  const path = event?.req?.path ?? "";
  const action = (event?.req?.query?.action as string | undefined)?.toLowerCase();
  try {
    if (path.endsWith("/control") && action === "pause") {
      ingestionPaused = true;
      event.res.json({ paused: true, queuedEvents: await countStoredEvents() });
      return;
    }
    if (path.endsWith("/control") && action === "resume") {
      ingestionPaused = false;
      await processStoredEvents();
      event.res.json({ paused: false, queuedEvents: await countStoredEvents() });
      return;
    }
    if (path.endsWith("/control")) {
      event.res.json({ paused: ingestionPaused, queuedEvents: await countStoredEvents() });
      return;
    }
    if (path.endsWith("/topics")) {
      event.res.json({
        activeTopics,
        topicMetadataCount: Object.keys(topicMetadata ?? {}).length,
        paused: ingestionPaused,
        queuedEvents: await countStoredEvents(),
        processedEventIdsSize: processedEventIds.size,
        lastTopicsRefreshAt: lastTopicsRefreshAt ? new Date(lastTopicsRefreshAt).toISOString() : null,
      });
      return;
    }
    event.res.status(404).json({ error: "Unknown control endpoint" });
  } catch (err) {
    const reason = err instanceof Error ? err.message : String(err);
    event.res.status(500).json({ error: reason });
  }
}

/**
 * Subscribes the MQTT client to the specified topics.
 * MAKE SURE to change instanceName of the UnsProxy to unsArchiverInputDev if testing locally
 * @param topics - The list of topics to subscribe to
 */
async function subscribeToTopics(topics: string[]) {
  // Topic filters
  const ds = config.questdb.dataStorage;
  const topicFilters: string[] = ds.map((item) => item.topic);

  const hasWildcards = topicFilters.some((filter) => filter.includes("#") || filter.includes("+"));

  // If we already subscribe to broad wildcard filters (recommended), adding concrete active topics is redundant
  // and can cause duplicate deliveries on some brokers.
  const requestedTopics = hasWildcards
    ? topicFilters.filter(Boolean)
    : Array.from(
        new Set([
          ...topicFilters,
          ...(topics ?? []).filter((topic) => topicFilters.some((topicFilter) => TopicMatcher.matches(topicFilter, topic))),
        ]),
      ).filter(Boolean);

  const desiredTopics = minimizeSubscriptionFilters(requestedTopics);

  if (desiredTopics.length < requestedTopics.length) {
    logger.info(
      `Reduced MQTT subscriptions from ${requestedTopics.length} to ${desiredTopics.length} by removing overlapping topic filters.`,
    );
  }

  if (desiredTopics.length === 0) {
    logger.warn("No topics to subscribe after filtering; skipping subscribe.");
    return;
  }

  const infraChannel = resolveMqttChannel(config.infra);
  const inputChannel = resolveMqttChannel(config.infra, config.input);

  if (!mqttInput) {
    const unsProxyProcess = new UnsProxyProcess(infraChannel.host, {
      processName: config.uns.processName,
      ...mqttChannelParameters(infraChannel),
    });
    mqttInput = await unsProxyProcess.createUnsMqttProxy(
      inputChannel.host,
      "unsArchiverInput",
      config.uns.instanceMode!,
      config.uns.handover!,
      {
        ...mqttChannelParameters(inputChannel),
        mqttSubToTopics: desiredTopics,
        subscribeThrottlingDelay: 0,
      }
    );
    mqttInput.event.on("input", async (mqttEvent) => {
      await processEvent(mqttEvent);
    });
    await publishQuestDbMapping();
  } else {
    mqttInput.subscribeAsync(desiredTopics);
  }
}

/**
 * Refreshes active topics and updates MQTT subscriptions if topics have changed.
 */
async function refreshActiveTopics() {
  try {
    const { topics: newActiveTopics, metaByTopic } = await ActiveUnsTopics.getActiveUnsTopics();
    if (JSON.stringify(newActiveTopics) !== JSON.stringify(activeTopics)) {
      logger.warn("Active topics changed. Updating subscriptions...");
      const oldActiveTopics = activeTopics;
      activeTopics = newActiveTopics;
      topicMetadata = metaByTopic;
      rebuildActiveTopicSet();
      lastTopicsRefreshAt = Date.now();
      if (mqttInput) {
        const topicsToUnsubscribe = oldActiveTopics.filter(Boolean);
        if (topicsToUnsubscribe.length > 0) {
          mqttInput.unsubscribeAsync(topicsToUnsubscribe);
        }
      }
      await subscribeToTopics(activeTopics);
      await flushInactiveBuffer();
      // Also try to flush any previously spooled events from local storage.
      await processStoredEvents();
    } else {
      topicMetadata = metaByTopic;
      rebuildActiveTopicSet();
      await flushInactiveBuffer();
    }
    if (!lastTopicsRefreshAt) {
      lastTopicsRefreshAt = Date.now();
    }
  } catch (error: any) {
    logger.error(`Failed to refresh active topics: ${error.message}`);
  }
}

/**
 * Processes a single MQTT event.
 * @param mqttEvent - The MQTT event to process.
 * @param fromStorage - Indicates if the event is from storage.
 * @returns A boolean indicating success or failure.
 */
async function processEvent(
  mqttEvent: { topic: any; message: any },
  fromStorage = false,
  options?: { bypassActiveTopicCheck?: boolean }
): Promise<boolean> {
  const inputTopic = mqttEvent.topic ?? "";
  const traceIngest = traceIngestEnabled;

  try {
    if (traceIngest) {
      const size =
        typeof mqttEvent.message === "string"
          ? mqttEvent.message.length
          : Buffer.isBuffer(mqttEvent.message)
            ? mqttEvent.message.length
            : JSON.stringify(mqttEvent.message ?? "").length;
      logger.info(
        `[trace][ingest] rx topic='${String(inputTopic)}' size=${size} fromStorage=${fromStorage} at=${new Date().toISOString()}`,
      );
    }

    // Generate a unique ID for the event
    const eventId = generateEventId(mqttEvent);

    if (ingestionPaused) {
      await saveEventToFile(mqttEvent, eventId);
      logger.debug("Archiver paused; event queued to local storage.");
      return true;
    }

    // Best-effort de-duplication. Note: EventEmitter won't await async handlers, so identical messages
    // can race; we guard with an inflight set.
    if (processedEventIds.has(eventId) || inflightEventIds.has(eventId)) {
      if (traceIngest) {
        logger.info(`[trace][ingest] dup_skip eventId=${eventId} topic='${String(inputTopic)}'`);
      } else {
        logger.info(`Event with ID ${eventId} on topic '${inputTopic}' has already been processed. Skipping.`);
      }
      return true;
    }
    inflightEventIds.add(eventId);
    const storage = TopicMatcher.findStorage(config, inputTopic);
    if (!storage) {
      logger.debug(`No matching table for topic ${inputTopic}`);
      inflightEventIds.delete(eventId);
      return true; // No need to retry
    }
    const matchingTable = storage.tablePrefix;

    const normalizedInputTopic = sanitizeTopicName(String(inputTopic ?? ""));
    const isActiveTopic = activeTopicSet.has(normalizedInputTopic);
    const bypassActive = !!options?.bypassActiveTopicCheck;
    if (!bypassActive && !isActiveTopic) {
      if (traceIngest) {
        logger.info(
          `[trace][buffer] inactive topic='${normalizedInputTopic}' eventId=${eventId} fromStorage=${fromStorage} activeTopicsCount=${activeTopicSet.size}`,
        );
      }

      if (fromStorage) {
        // Keep the file in storage for later retry after topics refresh.
        inflightEventIds.delete(eventId);
        return false;
      }

      // Buffer in memory; spill to local storage on overflow/expiry.
      await bufferInactiveEvent(normalizedInputTopic, mqttEvent, eventId);
      inflightEventIds.delete(eventId);
      return true;
    }


    // Handle UNS packet
    const unsPacket = UnsPacket.parseMqttPacket(mqttEvent.message);
    if (!unsPacket) {
      logger.warn(`Dropping invalid/unsupported UNS packet on topic '${inputTopic}'.`);
      inflightEventIds.delete(eventId);
      return true;
    }

    if (!unsPacket.message.data && !unsPacket.message.table) {
      logger.debug(`Dropping non data/table UNS packet on topic '${inputTopic}'.`);
      inflightEventIds.delete(eventId);
      return true;
    }

    let mappingUpdated = false;
    if (unsPacket.message.data) {
      mappingUpdated = recordObservedDataGroup(inputTopic, "data", unsPacket.message.data.dataGroup) || mappingUpdated;
    }
    if (unsPacket.message.table) {
      mappingUpdated = recordObservedDataGroup(inputTopic, "table", unsPacket.message.table.dataGroup) || mappingUpdated;
    }
    if (mappingUpdated) {
      await publishQuestDbMapping();
    }

    const meta =
      topicMetadata[inputTopic] ??
      topicMetadata[sanitizeTopicName(inputTopic)] ??
      undefined;

    const ingestMode = resolveIngestMode(storage, unsPacket);

    if (traceIngest) {
      const kind = unsPacket.message.table ? "table" : unsPacket.message.data ? "data" : "other";
      const group = unsPacket.message.table?.dataGroup ?? unsPacket.message.data?.dataGroup ?? "";
      const time = unsPacket.message.table?.time ?? unsPacket.message.data?.time ?? "";
      const intervalStart = (unsPacket.message.table as any)?.intervalStart ?? (unsPacket.message.data as any)?.intervalStart ?? null;
      const intervalEnd = (unsPacket.message.table as any)?.intervalEnd ?? (unsPacket.message.data as any)?.intervalEnd ?? null;
      const windowStart = (unsPacket.message.table as any)?.windowStart ?? (unsPacket.message.data as any)?.windowStart ?? null;
      const windowEnd = (unsPacket.message.table as any)?.windowEnd ?? (unsPacket.message.data as any)?.windowEnd ?? null;
      const deleted = (unsPacket.message.table as any)?.deleted ?? (unsPacket.message.data as any)?.deleted ?? false;
      const tableColumns = (unsPacket.message.table as any)?.columns;
      const colsCount =
        tableColumns && typeof tableColumns === "object" && !Array.isArray(tableColumns)
          ? Object.keys(tableColumns).length
          : 0;
      logger.info(
        `[trace][ingest] parsed eventId=${eventId} kind=${kind} mode=${ingestMode ?? ""} tablePrefix=${matchingTable} dataGroup='${group}' time='${time}' interval=[${String(intervalStart)},${String(intervalEnd)}] window=[${String(windowStart)},${String(windowEnd)}] deleted=${deleted} cols=${colsCount}`,
      );
    }

    await questDbWriter.writeUnsPacket(unsPacket, matchingTable, inputTopic, meta, ingestMode);

    if (traceIngest) {
      logger.info(`[trace][ingest] wrote eventId=${eventId} topic='${String(inputTopic)}' at=${new Date().toISOString()}`);
    }

    // Mark the event as processed
    processedEventIds.add(eventId);
    inflightEventIds.delete(eventId);
    if (processedEventIds.size > PROCESSED_EVENTS_CACHE_SIZE) {
      const oldestEntry = processedEventIds.values().next();
      if (!oldestEntry.done) {
        processedEventIds.delete(oldestEntry.value);
      }
    }

    return true;
  } catch (error: any) {
    // Release inflight guard on failure
    try {
      const eventId = generateEventId(mqttEvent);
      inflightEventIds.delete(eventId);
    } catch {
      // ignore
    }

    logger.error(`Error processing event: ${error?.message ?? String(error)}`);
    if (error instanceof NonRetryableError) {
      logger.warn(`Dropping non-retryable event on topic '${inputTopic}': ${error.message}`);
      return true;
    }
    if (!fromStorage) {
      await saveEventToFile(mqttEvent, generateEventId(mqttEvent));
    }
    return false;
  }
}

async function ensureEventStorageDirectories(): Promise<void> {
  await fs.mkdir(EVENT_STORAGE_DIR, { recursive: true });
  await fs.mkdir(FAILED_EVENT_STORAGE_DIR, { recursive: true });
}

async function lockStoredEventFile(
  fileName: string,
): Promise<{ originalFilePath: string; processingFilePath: string } | null> {
  const originalFilePath = path.join(EVENT_STORAGE_DIR, fileName);
  const processingFilePath = `${originalFilePath}.${process.pid}.${Date.now()}${EVENT_PROCESSING_EXTENSION}`;
  try {
    await fs.rename(originalFilePath, processingFilePath);
    return { originalFilePath, processingFilePath };
  } catch (error: any) {
    if (error?.code === "ENOENT" || error?.code === "EPERM" || error?.code === "EACCES") {
      return null;
    }
    throw error;
  }
}

async function requeueProcessingFile(processingFilePath: string, originalFilePath: string): Promise<void> {
  try {
    await fs.rename(processingFilePath, originalFilePath);
  } catch (error: any) {
    const fallbackName = `${path.basename(originalFilePath, EVENT_FILE_EXTENSION)}-${Date.now()}${EVENT_FILE_EXTENSION}`;
    const fallbackPath = path.join(EVENT_STORAGE_DIR, fallbackName);
    try {
      await fs.rename(processingFilePath, fallbackPath);
      logger.warn(`Stored event lock rollback used fallback file name: ${fallbackName}`);
    } catch (fallbackError: any) {
      logger.error(`Failed to requeue stored event file ${processingFilePath}: ${fallbackError.message}`);
      await moveProcessingFileToFailed(processingFilePath, "requeue_failed");
    }
  }
}

async function moveProcessingFileToFailed(processingFilePath: string, reason: string): Promise<void> {
  const nowIso = new Date().toISOString().replace(/[:.]/g, "-");
  const baseName = path.basename(processingFilePath);
  const eventNameEnd = baseName.indexOf(EVENT_FILE_EXTENSION);
  const eventBaseName =
    eventNameEnd >= 0
      ? baseName.slice(0, eventNameEnd + EVENT_FILE_EXTENSION.length)
      : `${baseName}${EVENT_FILE_EXTENSION}`;
  const failedName = `${eventBaseName}.${sanitizeReason(reason)}.${nowIso}`;
  const failedPath = path.join(FAILED_EVENT_STORAGE_DIR, failedName);
  try {
    await ensureEventStorageDirectories();
    await fs.rename(processingFilePath, failedPath);
    logger.error(`Moved malformed stored event to ${failedPath}`);
  } catch (error: any) {
    logger.error(`Could not move stored event to failed folder: ${error.message}`);
    await fs.unlink(processingFilePath).catch(() => undefined);
  }
}

/**
 * Processes all stored events from the local filesystem.
 */
async function processStoredEvents() {
  if (ingestionPaused) {
    logger.info("Archiver paused; stored events remain queued.");
    return;
  }

  if (storedEventsRunInProgress) {
    logger.debug("Stored event processor is already running; skipping overlapping run.");
    return;
  }

  storedEventsRunInProgress = true;
  try {
    await ensureEventStorageDirectories();

    const files = await fs.readdir(EVENT_STORAGE_DIR);
    const eventFiles = files
      .filter((file) => path.extname(file) === EVENT_FILE_EXTENSION)
      .sort();

    if (eventFiles.length === 0) return;

    for (const file of eventFiles) {
      const lock = await lockStoredEventFile(file);
      if (!lock) continue;

      const { originalFilePath, processingFilePath } = lock;
      let fileContent = "";
      let mqttEvent: any = undefined;
      try {
        fileContent = await fs.readFile(processingFilePath, "utf-8");
        mqttEvent = JSON.parse(fileContent);
      } catch (error: any) {
        logger.error(`Could not parse stored event ${file}: ${error.message}`);
        logger.error(`Raw file content: ${fileContent}`);
        await moveProcessingFileToFailed(processingFilePath, "parse_error");
        continue;
      }

      try {
        const success = await processEvent(mqttEvent, true);
        if (success) {
          await fs.unlink(processingFilePath);
          logger.debug(`Processed stored event ${file}`);
        } else {
          logger.error(`Failed to process stored event ${file}, will retry later`);
          await requeueProcessingFile(processingFilePath, originalFilePath);
        }
      } catch (error: any) {
        logger.error(`Could not process stored event ${file}: ${error.message}`);
        await requeueProcessingFile(processingFilePath, originalFilePath);
      }
    }
  } catch (error: any) {
    logger.error(`Stored event processing loop failed: ${error.message ?? String(error)}`);
  } finally {
    storedEventsRunInProgress = false;
  }
}

/**
 * Generates a unique ID for the event.
 * @param mqttEvent - The MQTT event.
 * @returns A unique string ID.
 */
function generateEventId(mqttEvent: { topic: any; message: any }): string {
  // Combine topic and message data to create a unique ID
  const topic = mqttEvent.topic ?? "";
  const messageData = JSON.stringify(mqttEvent.message);
  const hash = hashString(topic + messageData);
  return hash;
}

/**
 * Hashes a string to create a unique representation.
 * @param str - The string to hash.
 * @returns A hash of the string.
 */
function hashString(str: string): string {
  return createHash("sha256").update(str).digest("hex");
}

async function countStoredEvents(): Promise<number> {
  try {
    await ensureEventStorageDirectories();
    const files = await fs.readdir(EVENT_STORAGE_DIR);
    return files.filter((file) => path.extname(file) === EVENT_FILE_EXTENSION).length;
  } catch {
    return 0;
  }
}

/**
 * Saves an event to a local file for later processing.
 * @param mqttEvent - The MQTT event to save.
 * @param eventId - The unique ID of the event.
 */
async function saveEventToFile(
  mqttEvent: any,
  eventId: string
) {
  // Attach the ID to the event
  const eventWithId = { id: eventId, ...mqttEvent };

  const uniqueFileName = `${eventId}${EVENT_FILE_EXTENSION}`;
  const finalFileName = path.join(EVENT_STORAGE_DIR, uniqueFileName);
  const tempFileName = path.join(
    EVENT_STORAGE_DIR,
    `${eventId}.${process.pid}.${Date.now()}.tmp`,
  );
  const data = JSON.stringify(eventWithId);

  logger.info(`Saving event to the local filesystem as ${finalFileName}`);
  try {
    await ensureEventStorageDirectories();
    try {
      await fs.access(finalFileName);
      logger.debug(`Event already queued on disk: ${uniqueFileName}`);
      return;
    } catch {
      // File does not exist, continue.
    }

    await fs.writeFile(tempFileName, data, { encoding: "utf-8", flag: "wx" });
    try {
      await fs.rename(tempFileName, finalFileName);
    } catch (error: any) {
      if (error?.code === "EEXIST") {
        await fs.unlink(tempFileName).catch(() => undefined);
        logger.debug(`Event already queued on disk during rename: ${uniqueFileName}`);
        return;
      }
      throw error;
    }
    logger.debug(`Event saved successfully to ${finalFileName}`);
  } catch (error: any) {
    logger.error(`Failed to save event to ${finalFileName}: ${error.message}`);
    await fs.unlink(tempFileName).catch(() => undefined);
  }
}

/**
 * Cleans up resources before exiting the application.
 */
async function cleanup() {
  if (mqttInput) {
    await mqttInput.stop();
  }
  if (questDbWriter) {
    await questDbWriter.close(); // Close the questDbOutput connection inside the QuestDBWriter
  }
  logger.warn("Cleanup completed. Exiting application.");
  process.exit(0);
}

process.on("SIGINT", cleanup);
process.on("SIGTERM", cleanup);
