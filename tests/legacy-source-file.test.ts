import { test, afterEach } from "node:test";
import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import path from "node:path";
import os from "node:os";
import { loadLegacySources, LEGACY_SOURCE_FILE } from "../src/legacy-source-file.js";
const roots: string[] = [];
async function fixture(value?: unknown) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "local-legacy-sources-")); roots.push(dir);
  if (value !== undefined) await fs.writeFile(path.join(dir, LEGACY_SOURCE_FILE), JSON.stringify(value));
  return dir;
}
afterEach(async () => { await Promise.all(roots.splice(0).map(dir => fs.rm(dir, { recursive: true, force: true }))); });
const source = { id: "retired-instance", directory: "/srv/openhub/retired/event_storage" };
test("local source provisioning works without controller env or callbacks", async () => {
  assert.deepEqual(await loadLegacySources(await fixture([source])), [source]);
});
test("missing local file preserves standalone config, not a new source", async () => {
  const dir = await fixture(); assert.deepEqual(await loadLegacySources(dir), []);
  assert.deepEqual(await loadLegacySources(dir, [source]), [source]);
});
test("local file and configured sources cannot silently override each other", async () => {
  await assert.rejects(loadLegacySources(await fixture([source]), [source]), /either local/);
});
test("source IDs and fields are validated; file content never appears in errors", async () => {
  for (const value of [{ password: "must-not-appear" }, [source, source], [{ ...source, directory: "relative" }], [{ ...source, token: "must-not-appear" }], [{ ...source, directory: "/srv/\0bad" }], Array.from({ length: 17 }, (_, i) => ({ ...source, id: `source-${i}` }))]) {
    await assert.rejects(loadLegacySources(await fixture(value)), /^Error: Invalid local legacy source file\.$/);
  }
});
test("corrupt and oversized files fail closed", async () => {
  const dir = await fixture();
  for (const content of ["{invalid must-not-appear", "x".repeat(65537)]) {
    await fs.writeFile(path.join(dir, LEGACY_SOURCE_FILE), content);
    await assert.rejects(loadLegacySources(dir), /^Error: Invalid local legacy source file\.$/);
  }
});
test("symlink, hardlink and nonregular local files fail closed", async () => {
  const dir = await fixture(); const other = await fixture([source]); const file = path.join(dir, LEGACY_SOURCE_FILE);
  await fs.symlink(path.join(other, LEGACY_SOURCE_FILE), file);
  await assert.rejects(loadLegacySources(dir), /unsafe/); await fs.unlink(file);
  await fs.link(path.join(other, LEGACY_SOURCE_FILE), file);
  await assert.rejects(loadLegacySources(dir), /Invalid/); await fs.unlink(file);
  await fs.mkdir(file); await assert.rejects(loadLegacySources(dir), /Invalid/);
});
