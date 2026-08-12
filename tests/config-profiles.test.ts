import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { composeConfigSchema } from "@uns-kit/core/uns-config/schema-tools.js";
import { unsCoreSchema } from "@uns-kit/core/uns-config/uns-core-schema.js";
import { projectExtrasSchema } from "../src/config/project.config.extension.js";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const schema = composeConfigSchema(unsCoreSchema, projectExtrasSchema).strict();

const profiles = [
  { file: "config-development-host.json", env: "dev", mqttHost: "localhost", questdbHost: "localhost:9000" },
  { file: "config-development-podman.json", env: "dev", mqttHost: "mosquitto", questdbHost: "questdb:9000" },
  { file: "config-production.json", env: "prod", mqttHost: "mosquitto", questdbHost: "questdb:9000" },
] as const;

test("configuration profiles are schema-valid and topology-specific", () => {
  for (const profile of profiles) {
    const config = JSON.parse(fs.readFileSync(path.join(repoRoot, profile.file), "utf8"));
    const result = schema.safeParse(config);
    assert.equal(result.success, true, result.success ? "" : `${profile.file}: ${result.error.message}`);
    assert.equal(config.uns.env, profile.env);
    assert.equal(config.infra.host, profile.mqttHost);
    assert.match(config.questdb.configurationString, new RegExp(profile.questdbHost.replace(/[.:]/g, "\\$&")));
    assert.equal("input" in config, false);
    assert.equal("output" in config, false);
    assert.equal("email" in config.uns, false);
    assert.equal("password" in config.uns, false);
  }
});
