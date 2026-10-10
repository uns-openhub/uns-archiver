import { promises as fs, type Dirent } from "node:fs";
import { performance } from "node:perf_hooks";

export const DEFAULT_DIRECTORY_SCAN_LIMITS = {
  maxEntries: 512,
  maxDurationMs: 25,
} as const;

export type DirectoryScanLimits = {
  maxEntries: number;
  maxDurationMs: number;
};

export type DirectoryCursor = {
  read(): Promise<Dirent | null>;
  close(): Promise<void>;
};

export type DirectoryScanPass = {
  entriesVisited: number;
  complete: boolean;
};

/** One open cursor per purpose, retained across bounded passes. No directory list in RAM. */
export class BoundedDirectoryScan {
  private cursor: DirectoryCursor | null = null;
  private activePass: Promise<DirectoryScanPass> | null = null;
  private closed = false;

  constructor(
    private readonly directory: string,
    private readonly openDirectory: (
      directory: string,
    ) => Promise<DirectoryCursor> = async (directory) =>
      await fs.opendir(directory, { bufferSize: 32 }),
    private readonly now: () => number = () => performance.now(),
  ) {}

  pass(
    visit: (entry: Dirent) => Promise<boolean | void> | boolean | void,
    limits: DirectoryScanLimits = DEFAULT_DIRECTORY_SCAN_LIMITS,
    shouldStop: () => boolean = () => false,
  ): Promise<DirectoryScanPass> {
    if (this.closed)
      return Promise.resolve({ entriesVisited: 0, complete: false });
    if (this.activePass) return this.activePass;
    if (
      !Number.isSafeInteger(limits.maxEntries) ||
      limits.maxEntries <= 0 ||
      !Number.isFinite(limits.maxDurationMs) ||
      limits.maxDurationMs <= 0
    ) {
      return Promise.reject(new Error("Invalid directory scan limits"));
    }
    const work = this.runPass(visit, limits, shouldStop);
    this.activePass = work;
    void work
      .finally(() => {
        if (this.activePass === work) this.activePass = null;
      })
      .catch(() => undefined);
    return work;
  }

  async close(): Promise<void> {
    this.closed = true;
    await this.activePass?.catch(() => undefined);
    await this.closeCursor();
  }

  private async runPass(
    visit: (entry: Dirent) => Promise<boolean | void> | boolean | void,
    limits: DirectoryScanLimits,
    shouldStop: () => boolean,
  ): Promise<DirectoryScanPass> {
    const started = this.now();
    let entriesVisited = 0;
    try {
      if (shouldStop()) return { entriesVisited, complete: false };
      if (!this.cursor) this.cursor = await this.openDirectory(this.directory);
      while (
        !this.closed &&
        !shouldStop() &&
        entriesVisited < limits.maxEntries &&
        this.now() - started < limits.maxDurationMs
      ) {
        const entry = await this.cursor.read();
        if (!entry) {
          await this.closeCursor();
          return { entriesVisited, complete: true };
        }
        entriesVisited++;
        if ((await visit(entry)) === false) break;
      }
      return { entriesVisited, complete: false };
    } catch (error) {
      await this.closeCursor();
      throw error;
    }
  }

  private async closeCursor(): Promise<void> {
    const cursor = this.cursor;
    this.cursor = null;
    if (cursor) {
      try {
        await cursor.close();
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ERR_DIR_CLOSED")
          throw error;
      }
    }
  }
}
