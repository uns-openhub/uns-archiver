import assert from "node:assert/strict";
import test from "node:test";
import { createServer } from "node:http";
import { mkdtempSync, rmSync } from "node:fs";
import path from "node:path";
import { tmpdir } from "node:os";
import { randomUUID } from "node:crypto";
import { Sender } from "@questdb/nodejs-client";
import { QuestDBWriter } from "../src/writers/questDbWriter.js";
import { deferredQuestDbSender } from "../src/writers/deferred-questdb-sender.js";

// Opt in against a disposable local database; never accept a remote destination.
const endpoint = process.env.ARCHIVER_TEST_QUESTDB;
test(
  "real QuestDB rows: pause one target, lose database transport for all, restart journal, recover and resume",
  { skip: !endpoint, timeout: 120000 },
  async () => {
    const url = new URL(endpoint!);
    assert.equal(url.hostname, "127.0.0.1");
    const root = mkdtempSync(path.join(tmpdir(), "archiver-target-questdb-"));
    const prefix = `test_target_${randomUUID().replaceAll("-", "")}`;
    let available = true;
    const transport = createServer(async (req, res) => {
      if (!available) {
        res.writeHead(503);
        res.end("Local outage injection");
        return;
      }
      try {
        const chunks: Buffer[] = [];
        for await (const chunk of req) chunks.push(Buffer.from(chunk));
        const result = await fetch(`${endpoint}${req.url}`, {
          method: req.method,
          headers: {
            "content-type": req.headers["content-type"] ?? "text/plain",
          },
          ...(req.method !== "GET" ? { body: Buffer.concat(chunks) } : {}),
        });
        res.writeHead(result.status, {
          "content-type": result.headers.get("content-type") ?? "text/plain",
        });
        res.end(Buffer.from(await result.arrayBuffer()));
      } catch {
        res.writeHead(503);
        res.end("Local database unreachable");
      }
    });
    await new Promise<void>((resolve) =>
      transport.listen(0, "127.0.0.1", resolve),
    );
    const configuration = `http::addr=127.0.0.1:${(transport.address() as any).port};retry_timeout=500;request_timeout=1000;`;
    const open = async () => {
      const writer = new QuestDBWriter(
        deferredQuestDbSender(configuration),
        configuration,
        { maxRows: 32, flushIntervalMs: 5 },
      );
      const journal = writer.configureTargetJournal({
        directory: path.join(root, "journal"),
        policyFile: path.join(root, "policy.json"),
        reserveBytes: 0,
        retryMs: 1,
        replayBatch: 64,
      });
      return { writer, journal };
    };
    let runtime = await open();
    const packet = (id: number) =>
      ({
        version: "2.0.0",
        message: {
          data: {
            time: new Date(1793650000000 + id * 1000).toISOString(),
            value: id,
            uom: "kWh",
          },
        },
      }) as any;
    const write = (group: string, id: number, occurrence = randomUUID()) =>
      runtime.writer.writeUnsPacket(
        packet(id),
        `${prefix}_${group}`,
        `canary/${group}/value`,
        undefined,
        undefined,
        undefined,
        { occurrenceId: occurrence },
      );
    const rows = async (group: string) => {
      const result = await fetch(
        `${endpoint}/exec?query=${encodeURIComponent(`select timestamp,numberValue,uom,fullTopic from ${prefix}_${group}_data order by timestamp`)}`,
      );
      const body = (await result.json()) as any;
      return result.ok ? body.dataset : [];
    };
    const waitRows = async (group: string, expected: number) => {
      for (let n = 0; n < 100; n++) {
        const data = await rows(group);
        if (data.length === expected) return data;
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      assert.fail(
        `Expected ${expected} ${group} rows, found ${(await rows(group)).length}`,
      );
    };
    try {
      await write("healthy", 1);
      await waitRows("healthy", 1);
      const command = {
        action: "pause",
        requestId: randomUUID(),
        expectedRevision: 0,
        reason: "Local test",
        selector: { tables: [`${prefix}_paused_data`] },
      };
      await runtime.journal.command(command, "local-operator");
      await write("paused", 2);
      await write("paused", 2);
      await write("healthy", 3);
      await waitRows("healthy", 2);
      assert.equal((await rows("paused")).length, 0);
      available = false;
      await Promise.all([
        write("paused", 4),
        write("healthy", 5),
        write("other", 6),
      ]);
      assert.equal(
        runtime.journal
          .snapshot()
          .targets.reduce((n, t) => n + Number(t.queued), 0),
        5,
      );
      await runtime.journal.close();
      await runtime.writer.close();
      runtime = await open();
      assert.equal(runtime.journal.isPaused(`${prefix}_paused_data`), true);
      await write("healthy", 7);
      await write("other", 8);
      await write("paused", 9);
      assert.equal(
        runtime.journal
          .snapshot()
          .targets.reduce((n, t) => n + Number(t.queued), 0),
        8,
      );
      available = true;
      for (let n = 0; n < 80; n++) {
        await runtime.journal.replay();
        if (
          Number(
            runtime.journal
              .snapshot()
              .targets.filter((t) => !t.paused)
              .reduce((sum, t) => sum + Number(t.queued), 0),
          ) === 0
        )
          break;
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      await waitRows("healthy", 4);
      await waitRows("other", 2);
      assert.equal((await rows("paused")).length, 0);
      assert.equal(
        runtime.journal.snapshot().targets.find((t) => t.paused)?.queued,
        4,
      );
      await runtime.journal.command(
        {
          action: "resume",
          pauseId: command.requestId,
          expectedRevision: 1,
          requestId: randomUUID(),
          reason: "Local resume",
        },
        "local-operator",
      );
      for (let n = 0; n < 5; n++) await runtime.journal.replay();
      const data = await waitRows("paused", 4);
      assert.deepEqual(
        data.map((row: any[]) => row.slice(1)),
        [
          [2, "kWh", "canary/paused/value"],
          [2, "kWh", "canary/paused/value"],
          [4, "kWh", "canary/paused/value"],
          [9, "kWh", "canary/paused/value"],
        ],
      );
      assert.equal(
        runtime.journal
          .snapshot()
          .targets.reduce((n, t) => n + Number(t.queued), 0),
        0,
      );
      const healthy = await rows("healthy");
      assert.deepEqual(
        healthy.map((row: any[]) => row[1]),
        [1, 3, 5, 7],
      );
      console.log(
        JSON.stringify({
          verification: "real-questdb-readback",
          healthyRows: healthy.length,
          resumedRows: data.length,
          otherRows: 2,
          queued: 0,
        }),
      );
    } catch (error) {
      console.error(error);
      throw error;
    } finally {
      await runtime.journal.close();
      await runtime.writer.close().catch(() => undefined);
      await new Promise<void>((resolve) => {
        transport.close(() => resolve());
        transport.closeAllConnections();
      });
      rmSync(root, { recursive: true, force: true });
    }
  },
);
