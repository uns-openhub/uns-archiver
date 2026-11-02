import {
  closeSync,
  fsyncSync,
  linkSync,
  mkdirSync,
  openSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";

/** Identity belongs to a received occurrence, never to its MQTT payload content. */
export function occurrenceId(event: Record<string, unknown>): string {
  const prior = event._archiverOccurrenceId ?? event.id; // backwards-compatible stored envelopes
  if (typeof prior === "string" && /^[a-zA-Z0-9-]{1,100}$/.test(prior))
    return prior;
  const id = randomUUID();
  event._archiverOccurrenceId = id;
  return id;
}

/** Return only after both file data and its directory entry are durable. Errors propagate. */
export function persistEventFile(
  directory: string,
  event: unknown,
  id: string,
): void {
  if (!/^[a-zA-Z0-9-]{1,100}$/.test(id))
    throw new Error("Invalid stored event identity");
  mkdirSync(directory, { recursive: true, mode: 0o700 });
  const temporary = path.join(directory, `${id}.${randomUUID()}.tmp`);
  const destination = path.join(directory, `${id}.event`);
  try {
    const descriptor = openSync(temporary, "wx", 0o600);
    try {
      writeFileSync(descriptor, JSON.stringify(event), "utf8");
      fsyncSync(descriptor);
    } finally {
      closeSync(descriptor);
    }
    // A retry must never replace an already acknowledged occurrence.
    try {
      linkSync(temporary, destination);
    } catch (error: any) {
      if (error.code !== "EEXIST") throw error;
    }
    const parent = openSync(directory, "r");
    try {
      fsyncSync(parent);
    } finally {
      closeSync(parent);
    }
  } finally {
    try {
      unlinkSync(temporary);
    } catch (error: any) {
      if (error.code !== "ENOENT") throw error;
    }
  }
}
