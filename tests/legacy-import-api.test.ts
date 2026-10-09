import assert from "node:assert/strict";
import test from "node:test";
import { legacyImportApi } from "../src/legacy-import-api.js";
function response() { return { statusCode: 200, body: undefined as any, status(code: number) { this.statusCode = code; return this; }, json(body: any) { this.body = body; return this; } }; }
test("legacy API keeps array status and command payload without operator metadata", async () => {
  const commands: any[] = [];
  const manager: any = { status: () => [{ sourceId: "old" }], command: async (body: any) => { commands.push(body); return body; } };
  const api = legacyImportApi(manager);
  const res = response(); api.get({ req: { query: {} }, res }); assert.ok(Array.isArray(res.body));
  const body = { action: "start", sourceId: "old", expectedRevision: 0, requestId: "request-1", confirmSourceClosed: true };
  await api.post({ req: { body }, res }); assert.deepEqual(commands, [body]);
  api.get({ req: { query: { format: "operator" } }, res }); assert.equal(res.statusCode, 503);
});
test("operator metadata is excluded from the manager idempotency payload", async () => {
  let submitted: any;
  const manager: any = { command: async (body: any) => submitted = body };
  const api = legacyImportApi(manager, { owner: { ownerId: "launch-1", processName: "a", controllerName: null, version: "1", instanceId: null }, acceptingCommands: () => true });
  const body = { action: "pause", sourceId: "old", requestId: "request-1", expectedRevision: 1 };
  const res = response(); await api.post({ req: { body: { ...body, expectedOwnerId: "launch-1" } }, res });
  assert.deepEqual(submitted, body);
  await api.post({ req: { body: { ...body, expectedOwnerId: 123 } }, res }); assert.equal(res.statusCode, 400);
});
