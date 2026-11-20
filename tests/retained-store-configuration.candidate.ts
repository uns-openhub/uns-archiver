import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import {
  mkdtempSync,
  readFileSync,
  realpathSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath, pathToFileURL } from "node:url";

const packageDirectory = realpathSync(
  fileURLToPath(new URL("..", import.meta.url)),
);
const candidate = process.env.UNS_RETAINED_CONFIGURATION_SDK_MODULE;
const sdk = await import(
  candidate
    ? pathToFileURL(candidate).href
    : "@uns-kit/core/runtime/retained-store.js"
);
assert.equal(
  typeof sdk.reviewRetainedStoreConfigurationChange,
  "function",
  "Use a built SDK with stopped-owner configuration review",
);
const { manifest } =
  sdk.readRetainedStoreConfigurationManifest(packageDirectory);
const productionProfile = JSON.parse(
  readFileSync(path.join(packageDirectory, "config-production.json"), "utf8"),
);

test("the package permits the stopped owner to change only the startup logging threshold", (t) => {
  assert.equal(manifest.activation, "stopped");
  const previous = {
    ...productionProfile,
    logging: { host: "127.0.0.1", level: "warn" },
  };
  const next = { ...previous, logging: { ...previous.logging, level: "info" } };
  assert.deepEqual(
    sdk.reviewRetainedStoreConfigurationChange(
      manifest,
      JSON.stringify(previous),
      JSON.stringify(next),
    ),
    {
      changedPaths: [["logging", "level"]],
    },
  );
  assert.throws(
    () =>
      sdk.reviewRetainedStoreConfigurationChange(
        manifest,
        JSON.stringify(productionProfile),
        JSON.stringify(next),
      ),
    /outside the retained store declaration/,
  );
  assert.throws(
    () =>
      sdk.reviewRetainedStoreConfigurationChange(
        manifest,
        JSON.stringify(previous),
        JSON.stringify({
          ...next,
          logging: { ...next.logging, host: "example.invalid" },
        }),
      ),
    /outside the retained store declaration/,
  );

  const instance = mkdtempSync(path.join(tmpdir(), "archiver-logging-level-"));
  t.after(() => rmSync(instance, { recursive: true, force: true }));
  const loggerUrl = import.meta.resolve("@uns-kit/core/logger.js");
  for (const config of [previous, next]) {
    writeFileSync(path.join(instance, "config.json"), JSON.stringify(config));
    const level = execFileSync(
      process.execPath,
      [
        "--input-type=module",
        "--eval",
        `import logger from ${JSON.stringify(loggerUrl)}; process.stdout.write(logger.level);`,
      ],
      {
        cwd: instance,
        encoding: "utf8",
      },
    );
    assert.equal(level, config.logging.level);
  }
});

for (const ingestMode of ["dedup", "append"]) {
  test(`rejects removing a group override in ${ingestMode} mode`, () => {
    const relatedGroup = { name: "meter_value_evidence", ingestMode: "append" };
    const storage = {
      topic: "example/plant/#",
      tablePrefix: "meter",
      ingestMode: "dedup",
      dataGroups: [{ name: "meter_value", ingestMode }, relatedGroup],
    };
    const previous = {
      ...productionProfile,
      questdb: { ...productionProfile.questdb, dataStorage: [storage] },
    };
    const next = {
      ...previous,
      questdb: {
        ...previous.questdb,
        dataStorage: [{ ...storage, dataGroups: [relatedGroup] }],
      },
    };
    assert.throws(
      () =>
        sdk.reviewRetainedStoreConfigurationChange(
          manifest,
          JSON.stringify(previous),
          JSON.stringify(next),
        ),
      /outside the retained store declaration/,
    );
  });
}
