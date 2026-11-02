import {
  TargetControlError,
  TargetJournal,
  targetCommandSchema,
} from "./target-journal.js";
import { z, ZodError } from "zod";

export const targetCommandOpenApiSchema = z.toJSONSchema(targetCommandSchema, {
  target: "draft-7",
});

/** Only call after UnsApiProxy has verified the JWT signature, expiry and path grant. */
export function verifiedOperator(request: any): string {
  const bearer = request?.headers?.authorization;
  if (typeof bearer !== "string" || !bearer.startsWith("Bearer "))
    throw new TargetControlError(401, "Authentication required");
  let claims: any;
  try {
    claims = JSON.parse(
      Buffer.from(bearer.slice(7).split(".")[1], "base64url").toString("utf8"),
    );
  } catch {
    throw new TargetControlError(401, "Invalid authenticated identity");
  }
  const roles = [
    claims.role,
    ...(Array.isArray(claims.roles) ? claims.roles : []),
  ];
  if (!roles.some((role) => role === "admin" || role === "operator"))
    throw new TargetControlError(403, "Operator role required");
  if (typeof claims.sub !== "string" || !claims.sub || claims.sub.length > 200)
    throw new TargetControlError(403, "Operator subject required");
  return claims.sub;
}

export function targetControlApi<T>(
  journal: TargetJournal<T>,
  acceptingCommands: () => boolean,
) {
  return {
    get(event: any) {
      try {
        verifiedOperator(event.req);
        event.res.json(journal.snapshot());
      } catch (error) {
        fail(event, error);
      }
    },
    async post(event: any) {
      try {
        const actor = verifiedOperator(event.req);
        const parsed = targetCommandSchema.parse(event.req.body);
        if (parsed.action !== "preview" && !acceptingCommands())
          throw new TargetControlError(
            409,
            "Archiver is not the active writable owner; read status before retrying",
          );
        const result = await journal.command(parsed, actor);
        event.res.json(result);
      } catch (error) {
        fail(event, error);
      }
    },
  };
}
function fail(event: any, error: unknown) {
  const status =
    error instanceof TargetControlError
      ? error.status
      : error instanceof ZodError
        ? 400
        : 503;
  event.res.status(status).json({
    error:
      error instanceof TargetControlError
        ? error.message
        : status === 400
          ? "Invalid target command"
          : "Target control unavailable; read status before retrying",
  });
}
