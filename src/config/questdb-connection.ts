export type QuestDbConnectionConfig = {
  configurationString?: string;
  url?: string;
  username?: string;
  password?: string;
};

const normalizeOptionalString = (value: string | undefined): string | undefined => {
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
 * Resolves either the legacy QuestDB client configuration string or the
 * structured URL/credential form into the string expected by QuestDB's ILP
 * sender. Structured credentials stay in process memory and are never used
 * for published mapping metadata.
 */
export const resolveQuestDbConfigurationString = (config: QuestDbConnectionConfig): string => {
  const configurationString = normalizeOptionalString(config.configurationString);
  if (configurationString) return configurationString;

  const url = normalizeOptionalString(config.url);
  const username = normalizeOptionalString(config.username);
  const password = normalizeOptionalString(config.password);
  if (!url || !username || !password) {
    throw new Error(
      "QuestDB requires either questdb.configurationString or questdb.url, questdb.username, and questdb.password",
    );
  }

  const parsed = parseQuestDbUrl(url);
  const protocol = parsed.protocol.slice(0, -1);
  return `${protocol}::addr=${parsed.host};username=${username};password=${password};auto_flush=on`;
};

/**
 * Metadata sent to the controller must never carry QuestDB credentials. The
 * controller only needs the endpoint in order to associate table mappings.
 */
export const resolveQuestDbPublicConfigurationString = (config: QuestDbConnectionConfig): string | undefined => {
  const configurationString = normalizeOptionalString(config.configurationString);
  if (configurationString) {
    const parts = configurationString
      .split(";")
      .map((part) => part.trim())
      .filter(Boolean)
      .filter((part) => !/^(?:username|password|token)\s*=/i.test(part));
    return parts.length ? parts.join(";") : undefined;
  }

  const url = normalizeOptionalString(config.url);
  if (!url) return undefined;
  const parsed = parseQuestDbUrl(url);
  const protocol = parsed.protocol.slice(0, -1);
  return `${protocol}::addr=${parsed.host};auto_flush=on`;
};
