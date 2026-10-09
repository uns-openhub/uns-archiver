import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { StoredEventReplay } from "../src/stored-event-replay.js";
import { storedReplayControlStatus } from "../src/stored-replay-control.js";

const waitFor = async (condition: () => boolean) => {
  const deadline = Date.now() + 5000;
  while (!condition()) {
    assert.ok(Date.now() < deadline);
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
};

test("status, pause and resume acknowledge while an actual replay writer is pending", async () => {
  const root = await fs.mkdtemp(
    path.join(os.tmpdir(), "archiver-control-test-"),
  );
  const events = path.join(root, "event_storage");
  await fs.mkdir(events);
  await fs.writeFile(path.join(events, "queued.event"), "{}");
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  let paused = false,
    writing = false,
    requests = 0;
  const replay = new StoredEventReplay({
    eventStorageDirectory: events,
    failedStorageDirectory: path.join(events, "failed"),
    eventFileExtension: ".event",
    processingExtension: ".processing",
    isReady: () => true,
    isStopping: () => paused,
    hasLiveHeadroom: () => true,
    getLimits: () => ({ batchSize: 1, concurrency: 1 }),
    processEvent: async () => {
      writing = true;
      await gate;
      return true;
    },
  });
  const dependencies = {
    getPaused: () => paused,
    setPaused: (value: boolean) => {
      paused = value;
    },
    snapshot: () => replay.snapshot(),
    requestReplay: () => {
      requests++;
      void replay.run();
    },
  };
  try {
    const run = replay.run();
    await waitFor(() => writing);
    assert.equal(
      storedReplayControlStatus(undefined, dependencies).queuedEvents,
      null,
    );
    assert.equal(storedReplayControlStatus("pause", dependencies).paused, true);
    assert.equal(
      storedReplayControlStatus("resume", dependencies).paused,
      false,
    );
    assert.equal(requests, 1);
    assert.equal(replay.diagnostics().inFlight, 1);
    assert.equal(
      storedReplayControlStatus(undefined, dependencies).queuedEventsCountKind,
      "unknown",
    );
    release();
    await run;
    assert.equal(replay.diagnostics().successful, 1);
  } finally {
    release();
    await replay.close();
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("status responds from cache even while the inventory filesystem read is blocked", async (t) => {
  const root = await fs.mkdtemp(
    path.join(os.tmpdir(), "archiver-inventory-test-"),
  );
  await fs.mkdir(path.join(root, "failed"));
  const original = fs.opendir.bind(fs);
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  let reading = false;
  t.mock.method(
    fs,
    "opendir",
    async (...args: Parameters<typeof fs.opendir>) => {
      const directory = await original(...args);
      return {
        read: async () => {
          reading = true;
          await gate;
          return directory.read();
        },
        close: async () => {
          await directory.close();
        },
      };
    },
  );
  const replay = new StoredEventReplay({
    eventStorageDirectory: root,
    failedStorageDirectory: path.join(root, "failed"),
    eventFileExtension: ".event",
    processingExtension: ".processing",
    isReady: () => true,
    isStopping: () => false,
    hasLiveHeadroom: () => true,
    getLimits: () => ({ batchSize: 1, concurrency: 1 }),
    processEvent: async () => true,
  });
  try {
    const inspection = replay.refreshQueueSnapshot();
    await waitFor(() => reading);
    for (let i = 0; i < 1000; i++) {
      const status = storedReplayControlStatus(undefined, {
        getPaused: () => false,
        setPaused: () => {},
        requestReplay: () => {},
        snapshot: () => replay.snapshot(),
      });
      assert.equal(status.queueInspection.scanComplete, false);
      assert.equal(status.queuedEventsCountKind, "lower-bound");
      assert.equal(status.queuedEvents, null);
    }
    release();
    await inspection;
    assert.equal(replay.snapshot().scanComplete, true);
  } finally {
    release();
    await replay.close();
    t.mock.restoreAll();
    await fs.rm(root, { recursive: true, force: true });
  }
});
