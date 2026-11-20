import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { once } from "node:events";
import * as fs from "node:fs";
import { createServer } from "node:http";
import { createServer as createTcpServer } from "node:net";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { isDeepStrictEqual } from "node:util";

const [controllerInput, archiveInput] = process.argv.slice(2);
assert.ok(
  controllerInput && archiveInput,
  "Usage: node --import tsx accept-retained-configuration.mjs <controller-checkout> <local-archiver-tarball>",
);
const controller = fs.realpathSync(controllerInput);
const archive = fs.realpathSync(archiveInput);
const archiver = fs.realpathSync(fileURLToPath(new URL("..", import.meta.url)));
const archiverPackage = JSON.parse(
  fs.readFileSync(path.join(archiver, "package.json"), "utf8"),
);
const controllerImport = (file) =>
  import(pathToFileURL(path.join(controller, file)).href);
const { retainedConfigurationHost } = await controllerImport(
  "src/retained-store/configuration-host.ts",
);
const { assertRetainedStoreActionAllowed } = await controllerImport(
  "src/retained-store/host.ts",
);
const { hashPortableMigrationConfig } = await controllerImport(
  "src/controller-cluster/rtt-process-migration-coordinator.ts",
);
const { Ajv } = await controllerImport("node_modules/ajv/dist/ajv.js");
const sdk = await controllerImport(
  "node_modules/@uns-kit/core/dist/runtime/retained-store.js",
);

const dir = fs.mkdtempSync(
  path.join(fs.realpathSync(tmpdir()), "archiver-retained-config-"),
);
assert.equal(archiverPackage.name, "uns-archiver");
assert.match(archiverPackage.version, /^\d+\.\d+\.\d+$/);
const deployment = {
  rttNode: archiverPackage.name,
  version: archiverPackage.version,
  instanceId: "canary-one",
};
const packageDirectory = path.join(
  dir,
  "rtt-nodes",
  deployment.rttNode,
  deployment.version,
);
const instance = path.join(
  packageDirectory,
  "instances",
  deployment.instanceId,
);
const retainedRoot = path.join(dir, "stores");
const legacy = path.join(dir, "legacy");
for (const p of [instance, retainedRoot, legacy])
  fs.mkdirSync(p, { recursive: true, mode: 0o700 });
execFileSync("tar", [
  "-xzf",
  archive,
  "--strip-components",
  "1",
  "-C",
  packageDirectory,
]);
const packedPackage = JSON.parse(
  fs.readFileSync(path.join(packageDirectory, "package.json"), "utf8"),
);
for (const key of ["name", "version", "dependencies", "unsDatahub"]) {
  assert.ok(
    isDeepStrictEqual(packedPackage[key], archiverPackage[key]),
    `Local archive and built checkout disagree on package metadata ${key}`,
  );
}
for (const file of [
  "config.schema.json",
  "retained-store.manifest.json",
  sdk.RETAINED_STORE_CONFIGURATION_MANIFEST,
]) {
  assert.ok(
    fs
      .readFileSync(path.join(packageDirectory, file))
      .equals(fs.readFileSync(path.join(archiver, file))),
    `Local archive and built checkout disagree on ${file}`,
  );
}
fs.cpSync(path.join(archiver, "dist"), path.join(packageDirectory, "dist"), {
  recursive: true,
});
fs.symlinkSync(
  path.join(archiver, "node_modules"),
  path.join(packageDirectory, "node_modules"),
  "dir",
);
const fromArchiver = (file) =>
  import(pathToFileURL(path.join(packageDirectory, file)).href);
const { ReplayControl } = await fromArchiver("dist/replay-control.js");
const { persistEventFile } = await fromArchiver("dist/durable-event-file.js");
const file = path.join(instance, "config.json");
const cleanups = [];
const sockets = [];
let app;
let launch;
const ilpWrites = [];
const listenHttp = async (handler) => {
  const server = createServer(handler);
  sockets.push(server);
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return server.address().port;
};
async function waitFor(label, predicate, milliseconds = 12_000) {
  const until = Date.now() + milliseconds;
  while (Date.now() < until) {
    if (await predicate()) return;
    await delay(80);
  }
  throw new Error(`${label} timed out. Fixture: ${dir}`);
}
function startChild(command, args, logName, options = {}) {
  const output = fs.openSync(path.join(dir, logName), "w", 0o600);
  const child = spawn(command, args, {
    stdio: ["ignore", output, output],
    ...options,
  });
  fs.closeSync(output);
  const exited = once(child, "exit");
  const stop = async () => {
    if (child.exitCode === null && child.signalCode === null)
      child.kill("SIGTERM");
    const result = await Promise.race([
      exited,
      delay(8_000, undefined, { ref: false }).then(() => null),
    ]);
    if (result) return result;
    child.kill("SIGKILL");
    return exited;
  };
  cleanups.push(stop);
  return { child, stop };
}
async function startArchiver(sequence) {
  launch = {
    ...deployment,
    routeLaunchId: randomUUID(),
    routeStartedAt: Date.now(),
    routeControllerName: "fixture-controller",
    pmId: 22,
    restarts: sequence - 1,
    status: "online",
    pid: null,
    exitCode: null,
  };
  const log = `app-${sequence}.log`;
  app = startChild(
    process.execPath,
    [
      path.join(
        packageDirectory,
        "tests/fixtures/archiver-process-bootstrap.mjs",
      ),
      path.join(packageDirectory, "dist/index.js"),
    ],
    log,
    {
      cwd: instance,
      env: {
        PATH: process.env.PATH,
        NODE_ENV: "test",
        UNS_HANDOVER_LAUNCH_ID: launch.routeLaunchId,
        RTT_INSTANCE_ID: deployment.instanceId,
        UNS_RETAINED_STORE_ROOT: retainedRoot,
        UNS_SERVICE_TOKEN_FILE: path.join(dir, "service-token"),
      },
    },
  );
  launch.pid = app.child.pid;
  await waitFor(`Archiver boot ${sequence}`, () => {
    const output = fs.readFileSync(path.join(dir, log), "utf8");
    if (app.child.exitCode !== null)
      throw Error(`Archiver exited during startup. Fixture: ${dir}\n${output}`);
    return output.includes("archiver-fixture-booted");
  });
  return fs.readFileSync(path.join(dir, log), "utf8");
}
async function stopArchiver() {
  const [exitCode, signal] = await app.stop();
  assert.equal(signal, null);
  assert.equal(exitCode, 0);
  launch = { ...launch, status: "stopped", pid: 0, exitCode };
}

try {
  const topic = "fixture/plant/meter/reading";
  const discoveryPort = await listenHttp((req, res) => {
    res.setHeader("content-type", "application/json");
    if (req.url === "/graphql") {
      req.resume();
      res.end(
        JSON.stringify({
          data: {
            GetUnsNodes: [
              {
                id: 1,
                type: "Attribute",
                fullTopic: topic,
                unsNode: "reading",
                attribute: "reading",
                asset: "fixture-meter",
                objectType: "meter",
                objectId: "main",
                attributeNeedsPersistance: true,
              },
            ],
          },
        }),
      );
    } else {
      res.end(JSON.stringify({ keys: [] }));
    }
  });
  const databasePort = await listenHttp(async (req, res) => {
    let body = "";
    for await (const chunk of req) body += chunk.toString();
    if (req.url?.startsWith("/write")) {
      ilpWrites.push(...body.trim().split("\n").filter(Boolean));
      res.writeHead(204).end();
    } else {
      res.setHeader("content-type", "application/json");
      const response =
        req.url === "/settings"
          ? { config: { "line.proto.support.versions": [1] } }
          : {
              query: "select 1",
              columns: [{ name: "1", type: "INT" }],
              dataset: [[1]],
              count: 1,
            };
      res.end(JSON.stringify(response));
    }
  });
  const availablePort = createTcpServer();
  availablePort.listen(0, "127.0.0.1");
  await once(availablePort, "listening");
  const mqttPort = availablePort.address().port;
  await new Promise((resolve) => availablePort.close(resolve));
  fs.writeFileSync(
    path.join(dir, "mosquitto.conf"),
    `listener ${mqttPort} 127.0.0.1\nallow_anonymous true\npersistence false\n`,
    { mode: 0o600 },
  );
  const broker = startChild(
    process.env.MOSQUITTO_BIN || "mosquitto",
    ["-c", path.join(dir, "mosquitto.conf")],
    "mosquitto.log",
  );
  await waitFor(
    "loopback Mosquitto",
    () =>
      broker.child.exitCode === null &&
      fs
        .readFileSync(path.join(dir, "mosquitto.log"), "utf8")
        .includes("running"),
  );

  const storage = {
    topic: "fixture/plant/#",
    tablePrefix: "retained_canary",
    ingestMode: "dedup",
    dataGroups: [
      { name: "meter_value", ingestMode: "dedup" },
      { name: "meter_value_evidence", ingestMode: "append" },
    ],
  };
  const previous = {
    uns: {
      graphql: `http://127.0.0.1:${discoveryPort}/graphql`,
      rest: `http://127.0.0.1:${discoveryPort}/api`,
      processName: "archiver-fixture",
      instanceMode: "force",
      handover: false,
      env: "test",
      jwksWellKnownUrl: `http://127.0.0.1:${discoveryPort}/api/.well-known/jwks.json`,
      supervisor: { enabled: false },
    },
    logging: { host: "127.0.0.1", level: "warn" },
    infra: { host: "127.0.0.1", port: mqttPort },
    archiver: { ingestConcurrency: 4, ingestQueueMaxEvents: 16 },
    questdb: {
      configurationString: `http::addr=127.0.0.1:${databasePort};auto_flush=off;request_timeout=1000;retry_timeout=250;`,
      batch: { maxRows: 4, flushIntervalMs: 20, maxPendingRows: 16 },
      dataStorage: [storage],
    },
  };
  const current = {
    ...previous,
    logging: { ...previous.logging, level: "info" },
  };
  const ruleRemoval = {
    ...current,
    questdb: {
      ...current.questdb,
      dataStorage: [{ ...storage, dataGroups: [storage.dataGroups[1]] }],
    },
  };
  const schema = JSON.parse(
    fs.readFileSync(path.join(packageDirectory, "config.schema.json"), "utf8"),
  );
  const validate = new Ajv({
    allErrors: true,
    strict: false,
    validateFormats: false,
  }).compile(schema);
  const checkSchema = (config) =>
    assert.equal(validate(config), true, JSON.stringify(validate.errors));
  [previous, current, ruleRemoval].forEach(checkSchema);
  const raw = JSON.stringify(previous);
  fs.writeFileSync(file, raw, { mode: 0o600 });
  const token = [
    "eyJhbGciOiJub25lIn0",
    Buffer.from(
      JSON.stringify({ sub: "synthetic-local-service", exp: 4_102_444_800 }),
    ).toString("base64url"),
    "fixture",
  ].join(".");
  fs.writeFileSync(path.join(dir, "service-token"), token, { mode: 0o600 });

  const event = {
    topic,
    _archiverOccurrenceId: "fixture-marked",
    message: JSON.stringify({
      version: "2.0.0",
      message: {
        data: {
          time: "2026-11-20T00:00:00.000Z",
          dataGroup: "meter_value",
          value: 73,
          uom: "kWh",
        },
      },
    }),
  };
  persistEventFile(
    path.join(legacy, "event_storage"),
    event,
    event._archiverOccurrenceId,
  );
  const manifest = sdk.readRetainedStoreManifest(packageDirectory);
  const move = sdk.prepareRetainedStoreMove(
    legacy,
    retainedRoot,
    randomUUID(),
    manifest,
  );
  sdk.applyRetainedStoreMove(move, "adopt");
  const binding = {
    protocol: 1,
    storeId: move.storeId,
    generation: move.generation,
    operationId: randomUUID(),
    directory: move.directory,
    format: manifest.format,
    configurationDigest: hashPortableMigrationConfig(raw),
    owner: deployment,
    maintenance: true,
  };
  sdk.writeRetainedStoreBinding(instance, binding);
  sdk.writeRetainedStoreJson(path.join(move.directory, "store.json"), binding);
  sdk.writeRetainedStoreJson(
    path.join(move.directory, "store-resources.json"),
    manifest,
  );
  const eventFile = path.join(
    move.directory,
    "events",
    `${event._archiverOccurrenceId}.event`,
  );
  const snapshot = () => ({
    inode: fs.statSync(eventFile).ino,
    directoryInode: fs.statSync(path.dirname(eventFile)).ino,
    digest: createHash("sha256")
      .update(fs.readFileSync(eventFile))
      .digest("hex"),
  });
  const pending = snapshot();
  const firstLog = await startArchiver(1);
  assert.ok(!firstLog.includes("Archiver configured topic filters"));
  await stopArchiver();
  assert.deepEqual(snapshot(), pending);
  const cleanLaunchId = launch.routeLaunchId;
  const host = {
    basePath: dir,
    retainedRoot,
    controllerName: "fixture-controller",
    processes: async () => [launch],
    state: () => ({ desiredRunning: false }),
    validate: async (location, config) => {
      assert.equal(location, packageDirectory);
      checkSchema(config);
    },
    authority: async () => ({ identityId: null }),
    origin: () => ({
      source: "fixture-local-pack",
      packageVersion: deployment.version,
    }),
    lock: async (_selector, work) => work(),
  };
  const call = (id, action, args = {}) =>
    retainedConfigurationHost(
      host,
      { ...deployment, id, action, ...args },
      "fixture-operator",
    );
  const id = randomUUID();
  const reviewed = await call(id, "review", {
    configuration: JSON.stringify(current),
  });
  assert.deepEqual(reviewed.changedPaths, ["/logging/level"]);
  assert.equal(
    reviewed.currentConfigurationDigest,
    binding.configurationDigest,
  );
  assert.notEqual(reviewed.configurationDigest, binding.configurationDigest);
  await assert.rejects(
    call(randomUUID(), "review", {
      configuration: JSON.stringify(ruleRemoval),
    }),
    /outside the retained store declaration/,
  );
  assert.deepEqual(snapshot(), pending);
  const applied = await call(id, "apply", {
    reviewedChecksum: reviewed.checksum,
  });
  assert.equal(applied.state, "applied");
  assert.deepEqual(
    await call(id, "resume", { reviewedChecksum: reviewed.checksum }),
    applied,
  );
  const currentBinding = sdk.loadRetainedStoreBinding(instance);
  assert.deepEqual(currentBinding, {
    ...binding,
    configurationDigest: reviewed.configurationDigest,
  });
  assert.equal(fs.readFileSync(file, "utf8"), JSON.stringify(current));
  assertRetainedStoreActionAllowed(instance, reviewed.configurationDigest);
  assert.deepEqual(snapshot(), pending);

  const secondLog = await startArchiver(2);
  assert.match(
    secondLog,
    /Archiver configured topic filters: fixture\/plant\/#/,
  );
  assert.notEqual(launch.routeLaunchId, cleanLaunchId);
  await stopArchiver();
  assert.deepEqual(snapshot(), pending);
  assert.equal(ilpWrites.length, 0);
  const replay = new ReplayControl(
    path.join(move.directory, "replay"),
    () => 0,
  );
  assert.equal(replay.snapshot().paused, true);
  replay.command(
    {
      action: "resume",
      requestId: randomUUID(),
      expectedRevision: replay.snapshot().revision,
      reason: "Local fixture verified",
    },
    "fixture-operator",
  );
  await startArchiver(3);
  await waitFor(
    "marked row delivered to stub QuestDB sink",
    () => ilpWrites.length > 0,
    40_000,
  );
  await waitFor("marked event acknowledged", () => !fs.existsSync(eventFile));
  assert.equal(ilpWrites.length, 1);
  assert.match(ilpWrites[0], /^retained_canary_meter_value_data[, ]/);
  assert.match(ilpWrites[0], /value=73(?:[,. ]|$)/);
  assert.match(ilpWrites[0], /fixture\/plant\/meter\/reading/);
  assert.match(ilpWrites[0], /kWh/);
  assert.match(
    ilpWrites[0],
    new RegExp(`${Date.parse("2026-11-20T00:00:00.000Z")}000000$`),
  );
  await stopArchiver();
  const evidence = JSON.stringify(
    {
      fixtureDirectory: dir,
      packageVersion: deployment.version,
      changedPaths: reviewed.changedPaths,
      configurationDigestChanged: true,
      sameBindingExceptConfigurationDigest: true,
      fixtureEventInodeAndBytesPreserved: true,
      rejectedRedundantGroupRemoval: true,
      cleanArchiverLaunches: 3,
      realMosquitto: true,
      fixtureIlpAcknowledgedRows: ilpWrites.length,
      mockedLayers: [
        "Controller launch inventory adapter",
        "authority and installed-origin adapters",
        "outer instance lock adapter",
        "Controller GraphQL discovery",
        "QuestDB endpoint",
      ],
    },
    null,
    2,
  );
  fs.writeFileSync(path.join(dir, "acceptance.json"), `${evidence}\n`, {
    mode: 0o600,
  });
  console.log(evidence);
} finally {
  for (const stop of cleanups.reverse()) await stop();
  for (const server of sockets) {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  }
}
