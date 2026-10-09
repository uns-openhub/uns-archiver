import { promises as fs } from "node:fs";
import path from "node:path";
import { HandoverManager } from "@uns-kit/core/uns/handover-manager.js";
import { PACKAGE_INFO } from "@uns-kit/core/uns/process-config.js";
import { MqttTopicBuilder } from "@uns-kit/core/uns-mqtt/mqtt-topic-builder.js";
import { LegacyImportManager } from "../../src/legacy-import.js";
import { createArchiverShutdown } from "../../src/archiver-shutdown.js";

const [root, mode] = process.argv.slice(2);
// The microservice owns its lifecycle; no controller endpoint or IPC command
// participates in the source release, application drain or process exit.
for (const key of ["UNS_CONTROLLER_NAME", "UNS_CONTROLLER_HOST", "UNS_CONTROLLER_PORT", "UNS_CONTROLLER_PUBLIC_BASE"])
  if (process.env[key]) throw new Error("Standalone fixture must not have a controller endpoint");
let released = false;
let entered!: () => void;
const entry = new Promise<void>((resolve) => {
  entered = resolve;
});
let finish!: () => void;
const pending = new Promise<void>((resolve) => {
  finish = resolve;
});
const imports = new LegacyImportManager({
  sources: [{ id: "old", directory: path.join(root, "old") }],
  liveDirectory: path.join(root, "live"),
  instanceId: "child",
  settings: { batchSize: 1, concurrency: 1, intervalMs: 100 },
  policyDigest: () => "policy",
  canWrite: () => !released,
  hasLiveHeadroom: () => true,
  write: async () => {
    entered();
    await pending;
    return { outcome: "written", rows: 1 };
  },
});
await imports.command({
  action: "start",
  sourceId: "old",
  requestId: "child-start",
  expectedRevision: 0,
  confirmSourceClosed: true,
});
const replay = imports.tick();
await entry;
const shutdown = createArchiverShutdown(
  () => {
    released = true;
  },
  {
    stopMqtt: async () => undefined,
    waitForLiveIngest: async () => undefined,
    waitForStoredReplay: async () => {
      await imports.close();
      await replay;
    },
    closeQuestDb: async () => {
      if (mode === "failure") throw new Error("sensitive hook error text");
      await fs.writeFile(path.join(root, "writer-closed"), "yes");
    },
  },
);
if (mode.startsWith("signal-")) {
  const signal = mode.slice("signal-".length);
  if (signal !== "SIGINT" && signal !== "SIGTERM") throw new Error("Unsupported fixture signal");
  process.on(signal, async () => {
    setTimeout(finish, 100); // The already accepted write finishes locally.
    try {
      await shutdown.drain();
      if (!released) throw new Error("Admission did not stop");
      console.log("Standalone signal drain completed");
      process.exit(0);
    } catch { process.exit(1); }
  });
  // Model the live MQTT socket that keeps a standalone service alive.
  setInterval(() => {}, 1_000);
  await fs.writeFile(path.join(root, "signal-ready"), "yes");
  await new Promise(() => {});
}
const manager = new HandoverManager(
  "test",
  "source",
  { publish: async () => undefined } as any,
  [],
  false,
  true,
  false,
  undefined,
  { onRelease: shutdown.release, drain: shutdown.drain, timeoutMs: 1_000 },
);
// Isolate the exit/drain boundary; broker discovery and worker queues are tested separately.
(manager as any).active = true;
let passiveObserved = false;
manager.event.on("handoverManager", state => { if (!state.active) passiveObserved = true; });
const topic = `uns-infra/${MqttTopicBuilder.sanitizeTopicPart(PACKAGE_INFO.name)}/${MqttTopicBuilder.sanitizeTopicPart(PACKAGE_INFO.version)}/test/handover`;
const event = (type: string) => ({
  topic,
  message: JSON.stringify({ type, handoverId: "migration" }),
  packet: {
    properties: {
      responseTopic: "uns-infra/target/next/test/handover",
      userProperties: { processId: "target", processName: "test" },
    },
  },
});
await manager.handleMqttMessage(event("handover_request"));
if (!released || !passiveObserved) throw new Error("Standalone lifecycle did not publish passive/released state");
if (mode !== "timeout") setTimeout(finish, 100);
await manager.handleMqttMessage(event("handover_ack"));
throw new Error("Source did not exit");
