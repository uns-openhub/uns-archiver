import {
  LegacyImportError,
  type LegacyImportManager,
} from "./legacy-import.js";

// Authentication and path grants are applied by UnsApiProxy before these handlers.
export function legacyImportApi(manager: LegacyImportManager) {
  const failure = (res: any, error: unknown) =>
    res
      .status(error instanceof LegacyImportError ? error.status : 503)
      .json({
        error:
          error instanceof LegacyImportError
            ? error.code
            : "import-control-unavailable",
      });
  return {
    get(event: any) {
      try {
        const action = event.req.query.action;
        if (action && action !== "inspect" && action !== "status")
          throw new LegacyImportError("invalid-action", 400);
        event.res.json(
          action === "inspect"
            ? manager.inspect(String(event.req.query.sourceId ?? ""))
            : manager.status(),
        );
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
              ].includes(key),
          ) ||
          typeof body.sourceId !== "string" ||
          typeof body.requestId !== "string"
        )
          throw new LegacyImportError("invalid-command", 400);
        event.res.json(await manager.command(body));
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
    expectedRevision: { type: "integer", minimum: 0 },
    confirmSourceClosed: {
      type: "boolean",
      description:
        "Operator confirmation that the old MQTT/replay writer has exited. Required true for start/resume.",
    },
  },
};
