import { UnsPacket } from "@uns-kit/core/uns/uns-packet.js";
import type { IUnsPacket } from "@uns-kit/core";
import { NonRetryableError } from "./errors.js";

export type LegacyWriteResult =
  | { outcome: "written"; rows?: number; oldestEventTime?: string }
  | { outcome: "duplicate" }
  | { outcome: "deferred"; reason: string }
  | { outcome: "quarantined"; reason: string };

export type LegacyEvent = {
  topic: string;
  message: unknown;
  [key: string]: unknown;
};

/** Use the SDK's throwing outbound validator to avoid its inbound parser logging payload excerpts. */
export async function parseLegacyPacket(message: unknown): Promise<IUnsPacket> {
  const record = (value: unknown): value is Record<string, any> =>
    !!value && typeof value === "object" && !Array.isArray(value);
  if (
    record(message) &&
    message.type === "Buffer" &&
    Array.isArray(message.data)
  ) {
    if (
      !message.data.every(
        (value: unknown) =>
          Number.isInteger(value) && Number(value) >= 0 && Number(value) <= 255,
      )
    )
      throw new Error("Invalid buffer envelope");
    message = Buffer.from(message.data).toString("utf8");
  }
  const raw = typeof message === "string" ? JSON.parse(message) : message;
  if (
    !record(raw) ||
    typeof raw.version !== "string" ||
    (raw.version !== "2.0.0" &&
      !/^1\.\d+\.\d+(?:[-+].*)?$/.test(raw.version)) ||
    !record(raw.message)
  )
    throw new Error("Invalid wire envelope");
  const input = { ...raw.message };
  if (input.table && Array.isArray(input.table.columns)) {
    const columns: Record<string, unknown> = Object.create(null);
    for (const column of input.table.columns) {
      if (
        !record(column) ||
        typeof column.name !== "string" ||
        Object.hasOwn(columns, column.name)
      )
        throw new Error("Invalid legacy columns");
      const { name, ...value } = column;
      columns[name] = value;
    }
    input.table = { ...input.table, columns };
  }
  const validated = await UnsPacket.unsPacketFromUnsMessage(input as any);
  return {
    ...validated,
    version: raw.version,
    ...(typeof raw.interval === "number" ? { interval: raw.interval } : {}),
    message: {
      ...validated.message,
      ...(input.data ? { data: input.data } : {}),
    },
  };
}

/** Separate acknowledgement contract: a live spool/drop acknowledgement is not a DB write. */
export async function writeLegacyEvent(
  event: LegacyEvent,
  dependencies: {
    findStorage: (topic: string) => { ingestMode?: string } | null;
    getMode?: (packet: IUnsPacket) => string | undefined;
    write: (packet: IUnsPacket) => Promise<void>;
    parse?: (
      message: unknown,
    ) => IUnsPacket | null | Promise<IUnsPacket | null>;
  },
): Promise<LegacyWriteResult> {
  const storage = dependencies.findStorage(event.topic);
  if (!storage) return { outcome: "quarantined", reason: "no-storage-rule" };
  // Replaying an old window must not soft-delete today's rows.
  let packet: IUnsPacket | null;
  try {
    packet = await (dependencies.parse ?? parseLegacyPacket)(event.message);
  } catch {
    return { outcome: "quarantined", reason: "invalid-uns-packet" };
  }
  if (!packet || (!packet.message.data && !packet.message.table))
    return { outcome: "quarantined", reason: "unsupported-uns-packet" };
  if (
    (dependencies.getMode
      ? dependencies.getMode(packet)
      : storage.ingestMode) === "window_replace"
  )
    return { outcome: "deferred", reason: "window-replace-requires-review" };
  // Do not substitute current time for an absent/corrupt historical timestamp.
  for (const item of [packet.message.data, packet.message.table]) {
    if (item && (!item.time || !Number.isFinite(new Date(item.time).getTime())))
      return { outcome: "quarantined", reason: "invalid-event-time" };
  }
  try {
    await dependencies.write(packet); // QuestDBWriter resolves after shared ILP flush.
    const items = [packet.message.data, packet.message.table].filter(
      (item) => !!item,
    );
    return {
      outcome: "written",
      rows: items.length,
      oldestEventTime: new Date(
        Math.min(...items.map((item) => new Date(item!.time).getTime())),
      ).toISOString(),
    };
  } catch (error) {
    return error instanceof NonRetryableError
      ? { outcome: "quarantined", reason: "invalid-questdb-row" }
      : { outcome: "deferred", reason: "writer-unavailable" };
  }
}
