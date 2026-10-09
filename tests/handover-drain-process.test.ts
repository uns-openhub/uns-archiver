import assert from "node:assert/strict";
import test from "node:test";
import { promises as fs } from "node:fs";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { LegacyImportManager } from "../src/legacy-import.js";

for (const mode of ["success", "failure", "timeout"]) {
  test(
    `real source process ${mode}: drain exit and retained files are recoverable`,
    { timeout: 15_000 },
    async (t) => {
      const root = await fs.realpath(
        await fs.mkdtemp(path.join(tmpdir(), "handover-drain-")),
      );
      t.after(() => fs.rm(root, { recursive: true, force: true }));
      const source = path.join(root, "old");
      const live = path.join(root, "live");
      await fs.mkdir(source);
      await fs.mkdir(live);
      for (const name of ["first.event", "second.event"])
        await fs.writeFile(
          path.join(source, name),
          JSON.stringify({ topic: "test/topic", message: "{}" }),
        );
      const child = spawn(
        process.execPath,
        [
          "--import",
          fileURLToPath(
            new URL("../node_modules/tsx/dist/loader.mjs", import.meta.url),
          ),
          fileURLToPath(
            new URL("./fixtures/handover-drain-child.mts", import.meta.url),
          ),
          root,
          mode,
        ],
        { stdio: ["ignore", "pipe", "pipe"] },
      );
      let output = "";
      child.stdout.on("data", (chunk) => {
        output += String(chunk);
      });
      child.stderr.on("data", (chunk) => {
        output += String(chunk);
      });
      t.after(() => {
        if (child.exitCode === null) child.kill("SIGKILL");
      });
      const [code, signal] = await once(child, "exit");
      assert.equal(signal, null, output);
      assert.equal(code, mode === "success" ? 0 : 1, output);
      assert.equal(output.includes("sensitive hook error text"), false);
    assert.ok(output.includes(mode === "success" ? "source drain completed" : mode === "timeout" ? "source drain timed-out" : "source drain failed"), output);
      const remaining = (await fs.readdir(source)).filter((name) =>
        name.endsWith(".event"),
      );
      assert.equal(remaining.length, mode === "timeout" ? 2 : 1, output);
      if (mode === "success")
        assert.equal(
          await fs.readFile(path.join(root, "writer-closed"), "utf8"),
          "yes",
        );
      else
        await assert.rejects(fs.stat(path.join(root, "writer-closed")), {
          code: "ENOENT",
        });
      let writes = 0;
      const recovered = new LegacyImportManager({
        sources: [{ id: "old", directory: source }],
        liveDirectory: live,
        instanceId: "recovered",
        policyDigest: () => "policy",
        canWrite: () => true,
        hasLiveHeadroom: () => true,
        settings: { intervalMs: 100 },
        write: async () => {
          writes++;
          return { outcome: "written" };
        },
      });
      t.after(() => recovered.close());
      await recovered.inspect("old");
      await recovered.tick();
      const state = recovered.status()[0];
      assert.equal(state.job?.state, "paused");
      assert.equal(writes, 0);
      await recovered.command({
        action: "resume",
        sourceId: "old",
        expectedRevision: state.job!.revision,
        requestId: "reviewed-recovery",
        confirmSourceClosed: true,
      });
      for (
        let pass = 0;
        pass < 20 && recovered.status()[0].job?.state === "running";
        pass++
      ) {
        await recovered.tick();
        await new Promise((resolve) => setTimeout(resolve, 110));
      }
      assert.equal(recovered.status()[0].job?.state, "completed");
      assert.equal(writes, remaining.length);
      assert.equal(
        (await fs.readdir(source)).filter((name) => name.endsWith(".event"))
          .length,
        0,
      );
      await recovered.close();
    },
  );
}
