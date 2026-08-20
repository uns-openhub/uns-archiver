import type { SecretPlaceholder } from "@uns-kit/core/uns-config/secret-placeholders.js";

type QuestDbSecretValue = string | SecretPlaceholder;

export type QuestDbConnectionConfig = {
  configurationString?: string;
  url?: QuestDbSecretValue;
  username?: QuestDbSecretValue;
  password?: QuestDbSecretValue;
};

const normalizeOptionalString = (value: QuestDbSecretValue | undefined, field: string): string | undefined => {
  if (value !== undefined && typeof value !== "string") {
    throw new Error(`${field} secret reference was not resolved before QuestDB initialization`);
  }
  const normalized = value?.trim();
  return normalized || undefined;
};

const parseQuestDbUrl = (value: string): URL => {
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error("questdb.url must be an absolute http or https URL");
  }

  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    throw new Error("questdb.url must use http or https");
  }
  if (parsed.username || parsed.password) {
    throw new Error("questdb.url must not embed credentials; use questdb.username and questdb.password");
  }
  if (parsed.pathname !== "/" || parsed.search || parsed.hash) {
    throw new Error("questdb.url must be an origin URL without a path, query, or fragment");
  }
  return parsed;
};

/**
 * Batching owns transaction boundaries, so the sender itself must never flush
 * while a row is appended. Preserve all other legacy connection parameters.
 */
const withManualFlush = (configurationString: string): string => {
  const parts = configurationString
    .split(";")
    .map((part) => part.trim())
    .filter(Boolean)
    .filter((part) => !/^auto_flush\s*=/i.test(part));
  return [...parts, "auto_flush=off"].join(";");
};

/**
 * Resolves either the legacy QuestDB client configuration string or the
 * structured URL/credential form into the string expected by QuestDB's ILP
 * sender. Structured credentials stay in process memory and are never used
 * for published mapping metadata.
 */
export const resolveQuestDbConfigurationString = (config: QuestDbConnectionConfig): string => {
  const configurationString = normalizeOptionalString(config.configurationString, "questdb.configurationString");
  if (configurationString) return withManualFlush(configurationString);

  const url = normalizeOptionalString(config.url, "questdb.url");
  const username = normalizeOptionalString(config.username, "questdb.username");
  const password = normalizeOptionalString(config.password, "questdb.password");
  if (!url || !username || !password) {
    throw new Error(
      "QuestDB requires either questdb.configurationString or questdb.url, questdb.username, and questdb.password",
    );
  }

  const parsed = parseQuestDbUrl(url);
  const protocol = parsed.protocol.slice(0, -1);
  return `${protocol}::addr=${parsed.host};username=${username};password=${password};auto_flush=off`;
};

/**
 * Metadata sent to the controller must never carry QuestDB credentials. The
 * controller only needs the endpoint in order to associate table mappings.
 */
export const resolveQuestDbPublicConfigurationString = (config: QuestDbConnectionConfig): string | undefined => {
  const configurationString = normalizeOptionalString(config.configurationString, "questdb.configurationString");
  if (configurationString) {
    const parts = withManualFlush(configurationString)
      .split(";")
      .map((part) => part.trim())
      .filter(Boolean)
      .filter((part) => !/^(?:username|password|token)\s*=/i.test(part));
    return parts.length ? parts.join(";") : undefined;
  }

  const url = normalizeOptionalString(config.url, "questdb.url");
  if (!url) return undefined;
  const parsed = parseQuestDbUrl(url);
  const protocol = parsed.protocol.slice(0, -1);
  return `${protocol}::addr=${parsed.host};auto_flush=off`;
};
