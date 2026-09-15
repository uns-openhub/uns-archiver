import assert from "node:assert/strict";
import test from "node:test";
import { resolveControllerEndpoints } from "../src/controller-endpoints.js";

test("controller-managed runtime uses the launching controller base", () => {
  assert.deepEqual(
    resolveControllerEndpoints(
      {
        rest: "http://old-controller:3200/api",
        graphql: "http://old-controller:3200/graphql",
      },
      { UNS_CONTROLLER_PUBLIC_BASE: "http://drain-target:3202/" },
    ),
    {
      rest: "http://drain-target:3202/api",
      graphql: "http://drain-target:3202/graphql",
      managed: true,
    },
  );
});

test("direct development keeps configured controller endpoints", () => {
  assert.deepEqual(
    resolveControllerEndpoints(
      {
        rest: "http://localhost:3200/api",
        graphql: "http://localhost:3200/graphql",
      },
      {},
    ),
    {
      rest: "http://localhost:3200/api",
      graphql: "http://localhost:3200/graphql",
      managed: false,
    },
  );
});

test("invalid managed base cannot override valid direct configuration", () => {
  assert.equal(
    resolveControllerEndpoints(
      { rest: "http://localhost:3200/api" },
      { UNS_CONTROLLER_PUBLIC_BASE: "not a url" },
    ).rest,
    "http://localhost:3200/api",
  );
});
