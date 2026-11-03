import assert from "node:assert/strict";
import test from "node:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { createApplicationHandoverContract } from "@uns-kit/core";
import { HandoverManager } from "@uns-kit/core/uns/handover-manager.js";
import { MqttTopicBuilder } from "@uns-kit/core/uns-mqtt/mqtt-topic-builder.js";
import {
  ACTIVE_TIMEOUT,
  PACKAGE_INFO,
} from "@uns-kit/core/uns/process-config.js";
import {
  assertStoredQueueDrainedForHandover,
  createArchiverHandoverState,
} from "../src/archiver-handover-state.js";
import { TargetJournal } from "../src/target-journal.js";

test("a global pause blocks state export, validation and import, then resume permits them", () => {
  let paused = true;
  const calls: string[] = [];
  const state = createArchiverHandoverState({
    contract: "local-test",
    isPaused: () => paused,
    journal: {
      exportPolicy: () => {
        calls.push("export");
        return { revision: 0 };
      },
      validatePolicy: () => {
        calls.push("validate");
      },
      importPolicy: () => {
        calls.push("import");
      },
    },
  });
  assert.throws(() => state.exportState(), /Resume archiving/);
  assert.throws(() => state.validateState({}), /Resume archiving/);
  assert.throws(() => state.importState({}), /Resume archiving/);
  assert.deepEqual(calls, []);
  paused = false;
  assert.deepEqual(state.exportState(), { revision: 0 });
  state.validateState({});
  paused = true; // Recheck if a pause appears after the initial validation.
  assert.throws(() => state.importState({}), /Resume archiving/);
  assert.deepEqual(calls, ["export", "validate"]);
  paused = false;
  state.importState({});
  assert.deepEqual(calls, ["export", "validate", "import"]);
});

test("only the active source receives preliminary write allowance; final validation and import remain strict", () => {
  let active = true;
  const checks: unknown[] = [];
  const state = createArchiverHandoverState({
    contract: "test-source-readiness",
    isPaused: () => false,
    isSourceActive: () => active,
    journal: {
      exportPolicy: (options) => {
        checks.push(options);
        return {};
      },
      validatePolicy: (_value, options) => {
        checks.push(options);
      },
      importPolicy: () => {
        checks.push("strict-import");
      },
    },
  });
  state.exportState();
  state.validateState({});
  active = false;
  state.exportState();
  state.validateState({});
  state.importState({});
  assert.deepEqual(checks, [
    { sourcePreflight: true },
    { sourcePreflight: true },
    { sourcePreflight: false },
    { sourcePreflight: false },
    "strict-import",
  ]);
});

for (const kind of ["global", "target"] as const) {
  test(`published SDK rejects ${kind} pause before releasing the active source, including repeat requests`, async (t) => {
    const root = mkdtempSync(path.join(tmpdir(), "archiver-paused-handover-"));
    const rows: unknown[] = [];
    const journal = new TargetJournal({
      directory: path.join(root, "journal"),
      policyFile: path.join(root, "policy.json"),
      reserveBytes: 0,
      write: async (entry) => {
        rows.push(entry);
      },
    });
    t.after(async () => {
      await journal.close();
      rmSync(root, { recursive: true, force: true });
    });
    let paused = false,
      exports = 0,
      released = 0,
      drained = 0,
      active = false;
    const states: boolean[] = [];
    const application = createArchiverHandoverState({
      contract: createApplicationHandoverContract("archiver-target-policy-v1", {
        local: true,
      }),
      isPaused: () => paused,
      journal,
    });
    const exportState = application.exportState.bind(application);
    application.exportState = () => {
      exports++;
      return exportState();
    };
    // Use the real published protocol handler with an in-memory transport.
    // This is not a two-Controller or real-MQTT live-migration acceptance test.
    t.mock.timers.enable({ apis: ["setTimeout"] });
    const manager = new HandoverManager(
      "paused-archiver-test",
      "source-local",
      { publish: async () => undefined } as any,
      [],
      false,
      true,
      true,
      undefined,
      {
        onRelease: () => {
          released++;
        },
        drain: async () => {
          drained++;
        },
        timeoutMs: 1000,
      },
      true,
      application,
    );
    manager.event.on("handoverManager", (event) => {
      active = event.active;
      states.push(active);
    });
    t.mock.timers.tick(ACTIVE_TIMEOUT);
    for (let i = 0; i < 5; i++) await Promise.resolve();
    assert.equal(active, true);
    if (kind === "global") paused = true;
    else
      await journal.command(
        {
          action: "pause",
          selector: { tables: ["test_paused_data"] },
          requestId: randomUUID(),
          expectedRevision: 0,
          reason: "Maintenance",
        },
        "operator",
      );
    const topic = new MqttTopicBuilder(
      `uns-infra/${MqttTopicBuilder.sanitizeTopicPart(PACKAGE_INFO.name)}/${MqttTopicBuilder.sanitizeTopicPart(PACKAGE_INFO.version)}/paused-archiver-test/`,
    ).getHandoverTopic();
    const request = {
      topic,
      message: Buffer.from(
        JSON.stringify({
          type: "handover_request",
          protocol: 2,
          handoverId: "paused-operation",
          applicationContract: application.contract,
          offer: [],
        }),
      ),
      packet: {
        retain: false,
        properties: {
          responseTopic: "canary/local/reply",
          userProperties: {
            processId: "target-local",
            handoverId: "paused-operation",
          },
        },
      },
    } as any;
    await manager.handleMqttMessage(request);
    await manager.handleMqttMessage(request); // A lost response cannot bypass the pause.
    assert.equal(exports, 2);
    assert.deepEqual(states, [true]);
    assert.equal(active, true);
    assert.equal(released, 0);
    assert.equal(drained, 0);
    if (kind === "target") {
      await journal.submit([
        {
          id: randomUUID(),
          table: "test_healthy_data",
          topic: "canary/healthy/value",
          payload: { value: 1 },
        },
      ]);
      assert.equal(rows.length, 1);
    }
  });
}

test("file-queue readiness requires a complete fresh zero count, including after a new durable spill", () => {
  const status: any = {
    active: false,
    inFlight: 0,
    queueInspection: {
      scanComplete: true,
      countKind: "observed",
      queuedEvents: 0,
      processingFiles: 0,
      unresolvedProcessingFiles: 0,
      lastError: null,
      startedAt: "2026-11-03T07:00:00.000Z",
      capturedAt: "2026-11-03T07:00:00.010Z",
    },
  };
  assert.doesNotThrow(() => assertStoredQueueDrainedForHandover(status));
  for (const change of [
    { scanComplete: false },
    { countKind: "lower-bound" },
    { queuedEvents: 1 },
    { processingFiles: 1 },
    { unresolvedProcessingFiles: 1 },
    { lastError: "read failed" },
    { startedAt: null },
    { startedAt: "2026-11-03T07:00:00.011Z" },
    { capturedAt: null },
  ])
    assert.throws(
      () =>
        assertStoredQueueDrainedForHandover({
          ...status,
          queueInspection: { ...status.queueInspection, ...change },
        }),
      /not confirmed empty/,
    );
  assert.throws(
    () => assertStoredQueueDrainedForHandover({ ...status, inFlight: 1 }),
    /not confirmed empty/,
  );
  assert.throws(
    () =>
      assertStoredQueueDrainedForHandover(
        status,
        Date.parse("2026-11-03T07:00:01Z"),
      ),
    /not confirmed empty/,
  );
  for (const spill of ["2026-11-03T07:00:00.000Z", "2026-11-03T07:00:00.005Z"])
    assert.throws(() => assertStoredQueueDrainedForHandover(status, Date.parse(spill)), /not confirmed empty/);
});
