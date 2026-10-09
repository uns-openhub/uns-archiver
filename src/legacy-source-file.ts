import { constants, promises as fs } from "node:fs";
import path from "node:path";
import type { LegacySource } from "./legacy-import.js";

export const LEGACY_SOURCE_FILE = "legacy-import-sources.json";
const MAX_BYTES = 64 * 1024;

/** Startup-only local provisioning. Never fetched from a controller or MQTT. */
export async function loadLegacySources(
  instanceDirectory: string,
  configured: LegacySource[] = [],
): Promise<LegacySource[]> {
  let handle;
  try {
    handle = await fs.open(path.join(instanceDirectory, LEGACY_SOURCE_FILE), constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return configured;
    throw new Error("Local legacy source file is unavailable or unsafe.");
  }
  try {
    const stat = await handle.stat();
    if (!stat.isFile() || stat.nlink !== 1 || stat.size > MAX_BYTES)
      throw new Error("Invalid local legacy source file.");
    const buffer = Buffer.alloc(MAX_BYTES + 1);
    const { bytesRead } = await handle.read(buffer, 0, buffer.length, 0);
    if (bytesRead > MAX_BYTES) throw new Error("Invalid local legacy source file.");
    const value: unknown = JSON.parse(buffer.subarray(0, bytesRead).toString("utf8"));
    if (!Array.isArray(value) || value.length > 16 || value.some(source =>
      !source || typeof source !== "object" || Array.isArray(source) ||
      Object.keys(source).some(key => !["id", "directory"].includes(key)) ||
      typeof source.id !== "string" || !/^[a-zA-Z0-9][a-zA-Z0-9-]{0,63}$/.test(source.id) ||
      typeof source.directory !== "string" || !path.isAbsolute(source.directory) ||
      source.directory.includes("\0") || source.directory.length > 4096
    ) || new Set(value.map(source => source.id)).size !== value.length)
      throw new Error("Invalid local legacy source file.");
    if (configured.length) throw new Error("Legacy sources must use either local provisioning or local startup configuration, not both.");
    return value as LegacySource[];
  } catch (error) {
    // Do not leak source paths, file contents, or JSON parser snippets.
    if (error instanceof Error && error.message.startsWith("Legacy sources must")) throw error;
    throw new Error("Invalid local legacy source file.");
  } finally { await handle.close(); }
}
