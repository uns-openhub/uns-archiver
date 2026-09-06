import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { composeConfigSchema } from "@uns-kit/core/uns-config/schema-tools.js";
import { unsCoreSchema } from "@uns-kit/core/uns-config/uns-core-schema.js";
import { projectExtrasSchema } from "../src/config/project.config.extension.js";
import {
  resolveQuestDbConfigurationString,
  resolveQuestDbPublicConfigurationString,
} from "../src/config/questdb-connection.js";

const repoRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
);
const schema = composeConfigSchema(unsCoreSchema, projectExtrasSchema).strict();

const profiles = [
  {
    file: "config-development-host.json",
    env: "dev",
    mqttHost: "localhost",
    questdbHost: "localhost:9000",
    dataStorageTopic: "forge-group/#",
    replayBatchSize: 64,
  },
  {
    file: "config-development-podman.json",
    env: "dev",
    mqttHost: "mosquitto",
    questdbHost: "questdb:9000",
    dataStorageTopic: "forge-group/#",
    replayBatchSize: 8,
  },
  {
    file: "config-production.json",
    env: "prod",
    mqttHost: "mosquitto",
    questdbHost: "questdb:9000",
    dataStorageTopic: "forge-group/#",
    replayBatchSize: 64,
  },
] as const;

test("configuration profiles are schema-valid and topology-specific", () => {
  for (const profile of profiles) {
    const config = JSON.parse(
      fs.readFileSync(path.join(repoRoot, profile.file), "utf8"),
    );
    const result = schema.safeParse(config);
    assert.equal(
      result.success,
      true,
      result.success ? "" : `${profile.file}: ${result.error.message}`,
    );
    assert.equal(config.uns.env, profile.env);
    assert.equal(config.infra.host, profile.mqttHost);
    assert.match(
      config.questdb.configurationString,
      new RegExp(profile.questdbHost.replace(/[.:]/g, "\\$&")),
    );
    assert.equal(config.questdb.dataStorage[0]?.topic, profile.dataStorageTopic);
    assert.equal("input" in config, false);
    assert.equal("output" in config, false);
    assert.equal("email" in config.uns, false);
    assert.equal("password" in config.uns, false);
    assert.equal(
      config.archiver.storedReplayBatchSize,
      profile.replayBatchSize,
    );
    assert.equal(config.archiver.storedReplayIntervalMs, 5000);
    assert.equal(config.archiver.identityEnrichmentEnabled, false);
    assert.equal(config.archiver.identityResolutionRetryMaxAgeMs, 30000);
  }
});

test("the Podman profile keeps live ingest ahead of the local MQTT rate", () => {
  const config = JSON.parse(
    fs.readFileSync(
      path.join(repoRoot, "config-development-podman.json"),
      "utf8",
    ),
  );

  assert.deepEqual(config.archiver, {
    inactiveBufferMax: 2000,
    inactiveBufferMaxAgeMs: 300000,
    ingestQueueMaxEvents: 1024,
    ingestQueueMaxBytes: 33554432,
    ingestConcurrency: 64,
    storedReplayBatchSize: 8,
    storedReplayIntervalMs: 5000,
    identityEnrichmentEnabled: false,
    identityResolutionRetryMaxAgeMs: 30000,
    traceIngest: false,
  });
  assert.deepEqual(config.questdb.batch, {
    flushIntervalMs: 250,
    maxRows: 256,
    maxPendingRows: 2048,
  });
});

test("structured QuestDB credentials are schema-valid and stay out of published metadata", () => {
  const config = {
    uns: {
      graphql: "http://localhost:3200/graphql",
      rest: "http://localhost:3200/api",
      processName: "uns-archiver",
      env: "prod",
    },
    infra: { host: "mqtt" },
    questdb: {
      url: "https://questdb.example:9000",
      username: "archiver",
      password: "not-for-metadata",
      dataStorage: [{ tablePrefix: "uns_enterprise", topic: "enterprise/#" }],
    },
  };

  const result = schema.safeParse(config);
  assert.equal(
    result.success,
    true,
    result.success ? "" : result.error.message,
  );
  assert.equal(
    resolveQuestDbConfigurationString(config.questdb),
    "https::addr=questdb.example:9000;username=archiver;password=not-for-metadata;auto_flush=off",
  );
  const metadata = resolveQuestDbPublicConfigurationString(config.questdb);
  assert.equal(metadata, "https::addr=questdb.example:9000;auto_flush=off");
  assert.equal(metadata?.includes("not-for-metadata"), false);
  assert.equal(metadata?.includes("username="), false);
});

test("structured QuestDB credentials accept Infisical references", () => {
  const config = {
    uns: {
      graphql: "http://localhost:3200/graphql",
      rest: "http://localhost:3200/api",
      processName: "uns-archiver",
      env: "prod",
    },
    infra: { host: "mqtt" },
    questdb: {
      url: {
        provider: "infisical",
        path: "/db/qdb",
        key: "QDB_HTTP_URL",
        environment: "prod",
      },
      username: {
        provider: "infisical",
        path: "/db/qdb",
        key: "QDB_USER",
        environment: "prod",
      },
      password: {
        provider: "infisical",
        path: "/db/qdb",
        key: "QDB_PASS",
        environment: "prod",
      },
      dataStorage: [{ tablePrefix: "uns_enterprise", topic: "enterprise/#" }],
    },
  };

  const result = schema.safeParse(config);
  assert.equal(
    result.success,
    true,
    result.success ? "" : result.error.message,
  );
});

test("QuestDB requires one complete connection form", () => {
  const profile = JSON.parse(
    fs.readFileSync(path.join(repoRoot, "config-production.json"), "utf8"),
  );
  delete profile.questdb.configurationString;
  profile.questdb.url = "https://questdb.example:9000";
  profile.questdb.username = "archiver";
  const result = schema.safeParse(profile);
  assert.equal(result.success, false);
});

test("legacy QuestDB mapping metadata strips embedded credentials", () => {
  const config = {
    configurationString:
      "http::addr=questdb.example:9000;username=archiver;password=not-for-metadata;auto_flush=on",
  };
  assert.equal(
    resolveQuestDbConfigurationString(config),
    "http::addr=questdb.example:9000;username=archiver;password=not-for-metadata;auto_flush=off",
  );
  const metadata = resolveQuestDbPublicConfigurationString(config);
  assert.equal(metadata, "http::addr=questdb.example:9000;auto_flush=off");
});

test("QuestDB batching rejects an impossible pending-row limit", () => {
  const profile = JSON.parse(
    fs.readFileSync(path.join(repoRoot, "config-production.json"), "utf8"),
  );
  profile.questdb.batch = { maxRows: 256, maxPendingRows: 255 };

  const result = schema.safeParse(profile);
  assert.equal(result.success, false);
});

test("stored replay batch size must be a positive integer", () => {
  const profile = JSON.parse(
    fs.readFileSync(path.join(repoRoot, "config-production.json"), "utf8"),
  );
  profile.archiver.storedReplayBatchSize = 0;

  const result = schema.safeParse(profile);
  assert.equal(result.success, false);
});

test("stored replay interval rejects a busy-loop configuration", () => {
  const profile = JSON.parse(
    fs.readFileSync(path.join(repoRoot, "config-production.json"), "utf8"),
  );
  profile.archiver.storedReplayIntervalMs = 249;

  const result = schema.safeParse(profile);
  assert.equal(result.success, false);
});
