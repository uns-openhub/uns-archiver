import {
  LegacyImportError,
  type LegacyImportManager,
} from "./legacy-import.js";

export type LegacyImportOwner = {
  ownerId: string;
  processName: string;
  controllerName: string | null;
  version: string;
  instanceId: string | null;
};
export type LegacyImportOperator = {
  owner: LegacyImportOwner;
  acceptingCommands: () => boolean;
};

// Authentication and path grants are applied by UnsApiProxy before these handlers.
export function legacyImportApi(manager: LegacyImportManager, operator?: LegacyImportOperator) {
  const failure = (res: any, error: unknown) =>
    res
      .status(error instanceof LegacyImportError ? error.status : 503)
      .json({
        error:
          error instanceof LegacyImportError
            ? error.code
            : "import-control-unavailable",
      });
  const checkOwner = (expected: unknown, required = false) => {
    if (expected === undefined && !required) return;
    if (typeof expected !== "string" || expected.length > 64)
      throw new LegacyImportError("invalid-runtime-owner", 400);
    if (!operator || expected !== operator.owner.ownerId)
      throw new LegacyImportError("runtime-owner-changed");
  };
  return {
    get(event: any) {
      try {
        const action = event.req.query.action;
        if (action && action !== "inspect" && action !== "status")
          throw new LegacyImportError("invalid-action", 400);
        const format = event.req.query.format;
        if (format !== undefined && format !== "operator")
          throw new LegacyImportError("invalid-format", 400);
        if (format === "operator" && !operator)
          throw new LegacyImportError("operator-status-unavailable", 503);
        checkOwner(event.req.query.expectedOwnerId, action === "inspect" && format === "operator");
        if (action === "inspect" && operator && !operator.acceptingCommands())
          throw new LegacyImportError("runtime-released");
        const sources = action === "inspect"
            ? manager.inspect(String(event.req.query.sourceId ?? ""))
            : manager.status();
        event.res.json(format === "operator" ? {
          protocol: 1, owner: operator!.owner,
          acceptingCommands: operator!.acceptingCommands(), sources,
        } : sources);
      } catch (error) {
        failure(event.res, error);
      }
    },
    async post(event: any) {
      try {
        const body = event.req.body;
        if (
          !body ||
          typeof body !== "object" ||
          Array.isArray(body) ||
          Object.keys(body).some(
            (key) =>
              ![
                "action",
                "sourceId",
                "requestId",
                "expectedRevision",
                "confirmSourceClosed",
                "expectedOwnerId",
              ].includes(key),
          ) ||
          typeof body.sourceId !== "string" ||
          typeof body.requestId !== "string"
        )
          throw new LegacyImportError("invalid-command", 400);
        checkOwner(body.expectedOwnerId);
        if (operator && !operator.acceptingCommands())
          throw new LegacyImportError("runtime-released");
        const { expectedOwnerId: _owner, ...command } = body;
        event.res.json(await manager.command(command));
      } catch (error) {
        failure(event.res, error);
      }
    },
  };
}

export const importCommandSchema = {
  type: "object",
  additionalProperties: false,
  required: ["action", "sourceId", "requestId", "expectedRevision"],
  properties: {
    action: { type: "string", enum: ["start", "pause", "resume", "cancel"] },
    sourceId: {
      type: "string",
      description: "Configured local source ID; never a filesystem path.",
    },
    requestId: { type: "string", pattern: "^[a-zA-Z0-9][a-zA-Z0-9-]{0,63}$" },
    expectedOwnerId: {
      type: "string", maxLength: 64,
      description: "Launch identity from operator status. Rejects controls routed to another runtime.",
    },
    expectedRevision: { type: "integer", minimum: 0 },
    confirmSourceClosed: {
      type: "boolean",
      description:
        "Operator confirmation that the old MQTT/replay writer has exited. Required true for start/resume.",
    },
  },
};
