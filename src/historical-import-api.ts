import { z } from "zod";
import type { OperatorOperationDefinition } from "@uns-kit/core/runtime/operator-api.js";
import { markOperatorNotApplied } from "@uns-kit/api/operator-api-authority.js";
import { verifiedOperator } from "./target-control-api.js";
import { TargetControlError } from "./target-journal.js";
import { ImportError } from "./historical-import-source.js";
import type { HistoricalImport } from "./historical-import.js";

const instant = z.string().min(20).max(27);
const range = {
  profileId: z.string().min(1).max(64),
  from: instant,
  to: instant,
};
const preview = z.strictObject({ action: z.literal("preview"), ...range });
const start = z.strictObject({
  action: z.literal("start"),
  ...range,
  requestId: z.uuid(),
  previewDigest: z
    .string()
    .length(64)
    .refine((value) => /^[a-f0-9]{64}$/.test(value)),
});
const command = z.discriminatedUnion("action", [preview, start]);
export const historicalImportOpenApiSchema = z.toJSONSchema(command, {
  target: "draft-7",
});
export const historicalImportStatusOperations: OperatorOperationDefinition[] = [
  {
    protocol: 1,
    id: "history-import.status",
    title: "List historical import profiles and jobs",
    kind: "read",
    requestSchema: {
      type: "object",
      properties: {},
      additionalProperties: false,
    },
  },
];
export const historicalImportOperations: OperatorOperationDefinition[] = [
  {
    protocol: 1,
    id: "history-import.preview",
    title: "Preview historical database import",
    kind: "read",
    requestSchema: z.toJSONSchema(preview, { target: "draft-7" }),
    timeoutMs: 10000,
  },
  {
    protocol: 1,
    id: "history-import.start",
    title: "Start reviewed historical database import",
    kind: "command",
    requestSchema: z.toJSONSchema(start, { target: "draft-7" }),
    timeoutMs: 10000,
    writeDomain: "history-import",
    replay: "request-id-v1",
  },
];

function fail(event: any, error: unknown) {
  const status =
    error instanceof ImportError || error instanceof TargetControlError
      ? error.status
      : error instanceof z.ZodError
        ? 400
        : 503;
  if (status < 500) markOperatorNotApplied(event.res);
  event.res.status(status).json({
    error:
      error instanceof ImportError
        ? error.code
        : status === 400
          ? "invalid-historical-import-request"
          : status === 401 || status === 403
            ? "historical-operator-required"
            : "historical-import-unavailable",
  });
}

export function historicalImportApi(manager: HistoricalImport) {
  return {
    get(event: any) {
      try {
        verifiedOperator(event.req);
        event.res.json(manager.status());
      } catch (error) {
        fail(event, error);
      }
    },
    async post(event: any) {
      try {
        const actor = verifiedOperator(event.req);
        const input = command.parse(event.req.body);
        if (input.action === "preview") {
          event.res.json(await manager.preview(input));
        } else {
          // A 200 returns the durable job receipt, not an assertion that the
          // asynchronous database import has finished.
          event.res.json(await manager.start(input, actor));
        }
      } catch (error) {
        fail(event, error);
      }
    },
  };
}
