import assert from "node:assert/strict";
import test from "node:test";
import { ArchiverEntityBindingClient } from "../src/entity-binding-client.js";

const topic = "site/line-a/press-14/equipment/main/temperature";

function resolution(revision = "7") {
  return {
    topic,
    asOf: "2026-09-03T10:00:00.000Z",
    status: "resolved",
    stableEntityId: "11111111-1111-4111-8111-111111111111",
    entityTypeKey: "openhub.asset",
    bindingKind: "attribute-topic",
    matchedPath: topic,
    validFrom: "2026-09-03T09:00:00.000Z",
    validTo: "2026-09-03T11:00:00.000Z",
    timeBasis: "source-event-time",
    sourceCount: 1,
    revision,
    digest: `sha256:${"1".repeat(64)}`,
  };
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
}

test("resolves exact event-time binding and caches revision evidence", async () => {
  let now = 1_000;
  const requests: Array<{ headers: HeadersInit | undefined; body: string }> = [];
  const client = new ArchiverEntityBindingClient({
    graphqlUrl: "http://controller/graphql",
    tokenProvider: { getAccessToken: async () => "service-token" },
    fetchImpl: async (_input, init) => {
      requests.push({ headers: init?.headers, body: String(init?.body) });
      return jsonResponse({ data: { ResolveEntityObservationBindings: [resolution()] } });
    },
    now: () => now,
    cacheTtlMs: 1_000,
  });
  const first = await client.resolveTopic(topic, "2026-09-03T10:00:00Z");
  assert.equal(first.source, "controller");
  assert.equal(first.resolution?.bindingKind, "attribute-topic");
  assert.equal(first.resolution?.revision, "7");
  assert.match(JSON.stringify(requests[0]?.headers), /Bearer service-token/);
  assert.deepEqual(JSON.parse(requests[0]!.body).variables, {
    topics: [topic],
    asOf: "2026-09-03T10:00:00.000Z",
  });
  now += 500;
  const reused = await client.resolveTopic(topic, "2026-09-03T10:30:00Z");
  assert.equal(reused.source, "cache");
  assert.equal(reused.resolution?.asOf, "2026-09-03T10:30:00.000Z");
  assert.equal(requests.length, 1);
  await client.resolveTopic(topic, "2026-09-03T11:00:00Z");
  assert.equal(requests.length, 2);
});

test("reuses the freshest interval entry after an older cache entry expires", async () => {
  let now = 1_000;
  let calls = 0;
  const client = new ArchiverEntityBindingClient({
    graphqlUrl: "http://controller/graphql",
    tokenProvider: { getAccessToken: async () => "service-token" },
    fetchImpl: async () => {
      calls += 1;
      return jsonResponse({ data: { ResolveEntityObservationBindings: [resolution(String(calls))] } });
    },
    now: () => now,
    cacheTtlMs: 1_000,
  });

  await client.resolveTopic(topic, "2026-09-03T10:00:00Z");
  now = 2_500;
  await client.resolveTopic(topic, "2026-09-03T10:10:00Z");
  now = 2_600;
  const reused = await client.resolveTopic(topic, "2026-09-03T10:20:00Z");

  assert.equal(reused.source, "cache");
  assert.equal(reused.resolution?.revision, "2");
  assert.equal(calls, 2);
});

test("keeps event-time cache keys separate and evicts least-recently-used entries", async () => {
  let calls = 0;
  const client = new ArchiverEntityBindingClient({
    graphqlUrl: "http://controller/graphql",
    tokenProvider: { getAccessToken: async () => "service-token" },
    fetchImpl: async (_input, init) => {
      calls++;
      const asOf = JSON.parse(String(init?.body)).variables.asOf as string;
      return jsonResponse({ data: { ResolveEntityObservationBindings: [{
        ...resolution(String(calls)),
        asOf,
        validFrom: asOf,
        validTo: new Date(new Date(asOf).getTime() + 1).toISOString(),
      }] } });
    },
    maxCacheEntries: 2,
  });
  await client.resolveTopic(topic, "2026-09-03T10:00:00Z");
  await client.resolveTopic(topic, "2026-09-03T11:00:00Z");
  await client.resolveTopic(topic, "2026-09-03T12:00:00Z");
  assert.deepEqual(client.snapshot(), { entries: 2, maxEntries: 2 });
  await client.resolveTopic(topic, "2026-09-03T10:00:00Z");
  assert.equal(calls, 4);
});

test("uses stale revision evidence only inside the bounded outage window", async () => {
  let now = 1_000;
  let unavailable = false;
  const client = new ArchiverEntityBindingClient({
    graphqlUrl: "http://controller/graphql",
    tokenProvider: { getAccessToken: async () => "service-token" },
    fetchImpl: async () => {
      if (unavailable) throw new Error("controller unavailable");
      return jsonResponse({ data: { ResolveEntityObservationBindings: [resolution()] } });
    },
    now: () => now,
    cacheTtlMs: 100,
    staleIfErrorMs: 1_000,
  });
  await client.resolveTopic(topic, "2026-09-03T10:00:00Z");
  unavailable = true;
  now += 200;
  assert.equal((await client.resolveTopic(topic, "2026-09-03T10:00:00Z")).source, "stale-cache");
  now += 1_000;
  await assert.rejects(
    () => client.resolveTopic(topic, "2026-09-03T10:00:00Z"),
    /controller unavailable/,
  );
});

test("preserves non-exact and unresolved results without promoting them", async () => {
  const client = new ArchiverEntityBindingClient({
    graphqlUrl: "http://controller/graphql",
    tokenProvider: { getAccessToken: async () => "service-token" },
    fetchImpl: async () => jsonResponse({ data: { ResolveEntityObservationBindings: [{
      ...resolution(),
      bindingKind: "asset-prefix",
      matchedPath: "site/line-a/press-14",
    }] } }),
  });
  const result = await client.resolveTopic(topic, "2026-09-03T10:00:00Z");
  assert.equal(result.resolution?.status, "resolved");
  assert.equal(result.resolution?.bindingKind, "asset-prefix");
  assert.notEqual(result.resolution?.matchedPath, topic);
});

test("rejects unsafe topics and invalid timing limits before making requests", async () => {
  const options = {
    graphqlUrl: "http://controller/graphql",
    tokenProvider: { getAccessToken: async () => "service-token" },
    fetchImpl: async () => { throw new Error("must not fetch"); },
  };
  const client = new ArchiverEntityBindingClient(options);
  await assert.rejects(() => client.resolveTopic("site/+/temperature"), /concrete/);
  assert.throws(() => new ArchiverEntityBindingClient({ ...options, maxCacheEntries: 0 }), /limits/);
});
