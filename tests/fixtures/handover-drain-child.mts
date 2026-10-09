import { promises as fs } from "node:fs";
import path from "node:path";
import { HandoverManager } from "@uns-kit/core/uns/handover-manager.js";
import { PACKAGE_INFO } from "@uns-kit/core/uns/process-config.js";
import { MqttTopicBuilder } from "@uns-kit/core/uns-mqtt/mqtt-topic-builder.js";
import { LegacyImportManager } from "../../src/legacy-import.js";
import { createArchiverShutdown } from "../../src/archiver-shutdown.js";

const [root, mode] = process.argv.slice(2);
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
if (!released) throw new Error("Admission was not released");
if (mode !== "timeout") setTimeout(finish, 100);
await manager.handleMqttMessage(event("handover_ack"));
throw new Error("Source did not exit");
