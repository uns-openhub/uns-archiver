import * as fs from "node:fs";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { z } from "zod";
import type { OperatorOperationDefinition } from "@uns-kit/core/runtime/operator-api.js";
import { markOperatorNotApplied } from "@uns-kit/api/operator-api-authority.js";
import { verifiedOperator } from "./target-control-api.js";
import { TargetControlError } from "./target-journal.js";

export const replayCommandSchema = z
  .object({
    action: z.enum(["pause", "resume"]),
    requestId: z.uuid(),
    expectedRevision: z.number().int().nonnegative(),
    reason: z.string().trim().min(1).max(240),
  })
  .strict();
const receipt = z
  .object({
    requestId: z.uuid(),
    revision: z.number().int().nonnegative(),
    paused: z.boolean(),
  })
  .strict();
const stateSchema = z
  .object({
    protocol: z.literal(1),
    maintenanceOperationId: z.uuid().optional(),
    revision: z.number().int().nonnegative(),
    paused: z.boolean(),
    change: z
      .object({
        actor: z.string().min(1).max(200),
        reason: z.string().min(1).max(240),
        at: z.string().datetime(),
      })
      .nullable(),
    receipts: z
      .array(
        z.object({ hash: z.string().length(64), result: receipt }).strict(),
      )
      .max(128),
  })
  .strict();

/** Owned by the same exclusive instance owner as TargetJournal. Never transports payloads. */
export class ReplayControl {
  private state: z.infer<typeof stateSchema> = {
    protocol: 1,
    revision: 0,
    paused: false,
    change: null,
    receipts: [],
  };
  private fault = false;
  constructor(
    private file: string,
    private inFlight: () => number,
  ) {
    if (fs.existsSync(file)) {
      const stat = fs.lstatSync(file);
      if (
        !stat.isFile() ||
        stat.isSymbolicLink() ||
        stat.nlink !== 1 ||
        stat.size > 256 * 1024 ||
        stat.uid !== process.getuid?.() ||
        stat.mode & 0o077
      )
        throw Error("Unsafe replay control state");
      this.state = stateSchema.parse(JSON.parse(fs.readFileSync(file, "utf8")));
    }
  }
  get paused() {
    return this.fault || this.state.paused;
  }
  snapshot() {
    const inFlight = this.inFlight();
    return {
      protocol: 1,
      revision: this.state.revision,
      paused: this.paused,
      healthy: !this.fault,
      change: this.state.change,
      boundary: {
        phase: this.paused ? (inFlight ? "pausing" : "paused") : "running",
        inFlight,
      },
      storage: "instance/archiver-replay-control.json",
      requiresSameRetainedStorage: true,
    };
  }
  ensureMaintenancePause(operationId: string) {
    z.uuid().parse(operationId);
    if(this.state.maintenanceOperationId === operationId) return;
    this.command({action:"pause",requestId:operationId,expectedRevision:this.state.revision,reason:"Reviewed retained store adoption"},"retained-store",operationId);
  }
  command(input: unknown, actor: string, maintenanceOperationId?: string) {
    const command = replayCommandSchema.parse(input);
    z.string().min(1).max(200).parse(actor);
    const hash = createHash("sha256")
      .update(JSON.stringify([actor, command]))
      .digest("hex");
    const prior = this.state.receipts.find(
      (r) => r.result.requestId === command.requestId,
    );
    if (prior) {
      if (prior.hash !== hash)
        throw new TargetControlError(409, "Replay request ID conflict");
      return prior.result;
    }
    if (this.fault) throw Error("Replay storage unavailable");
    if (command.expectedRevision !== this.state.revision)
      throw new TargetControlError(409, "Replay revision changed; read status");
    const result = {
      requestId: command.requestId,
      revision: this.state.revision + 1,
      paused: command.action === "pause",
    };
    const next = stateSchema.parse({
      protocol: 1,
      ...(maintenanceOperationId ? {maintenanceOperationId} : this.state.maintenanceOperationId ? {maintenanceOperationId:this.state.maintenanceOperationId} : {}),
      revision: result.revision,
      paused: result.paused,
      change: { actor, reason: command.reason, at: new Date().toISOString() },
      receipts: [...this.state.receipts, { hash, result }].slice(-128),
    });
    const temporary = `${this.file}.${randomUUID()}.tmp`;
    try {
      const fd = fs.openSync(temporary, "wx", 0o600);
      try {
        fs.writeFileSync(fd, JSON.stringify(next));
        fs.fsyncSync(fd);
      } finally {
        fs.closeSync(fd);
      }
      fs.renameSync(temporary, this.file);
      const parent = fs.openSync(path.dirname(this.file), "r");
      try {
        fs.fsyncSync(parent);
      } finally {
        fs.closeSync(parent);
      }
      this.state = next;
    } catch (error) {
      this.fault = true;
      throw error;
    } finally {
      if (fs.existsSync(temporary)) fs.unlinkSync(temporary);
    }
    return result;
  }
}
const commandJson = z.toJSONSchema(replayCommandSchema, { target: "draft-7" });
export const replayStatusOperations: OperatorOperationDefinition[] = [
  {
    protocol: 1,
    id: "replay.status",
    title: "Read durable replay pause and boundary",
    kind: "read",
    requestSchema: {
      type: "object",
      properties: {},
      additionalProperties: false,
    },
  },
];
export const replayCommandOperations: OperatorOperationDefinition[] = [
  "pause",
  "resume",
].map((action) => ({
  protocol: 1,
  id: `replay.${action}`,
  title: `${action === "pause" ? "Pause" : "Resume"} durable queue replay`,
  kind: "command",
  writeDomain: "replay-policy",
  replay: "request-id-v1",
  requestSchema: {
    ...commandJson,
    properties: {
      ...commandJson.properties,
      action: { type: "string", const: action },
    },
  },
}));
export const replayCommandOpenApiSchema = commandJson;
export function replayControlApi(
  control: ReplayControl,
  active: () => boolean,
  retainedStore?: () => unknown,
) {
  const handle = (event: any, command: boolean) => {
    try {
      const actor = verifiedOperator(event.req);
      if (command && !active())
        throw new TargetControlError(409, "Archiver is not the active owner");
      event.res.json(
        command ? control.command(event.req.body, actor) : {...control.snapshot(), ...(retainedStore ? {retainedStore:retainedStore()} : {})},
      );
    } catch (error) {
      const status =
        error instanceof TargetControlError
          ? error.status
          : error instanceof z.ZodError
            ? 400
            : 503;
      if (status < 500) markOperatorNotApplied(event.res);
      event.res
        .status(status)
        .json({
          error:
            status < 500
              ? (error as Error).message
              : "Replay control unavailable; read status before retrying",
        });
    }
  };
  return {
    get: (event: any) => handle(event, false),
    post: (event: any) => handle(event, true),
  };
}
