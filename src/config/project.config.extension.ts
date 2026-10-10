import { z } from "zod";
import {
  secretPlaceholderSchema,
  secretValueSchema,
} from "@uns-kit/core/uns-config/secret-placeholders.js";

const nonEmptySecretValueSchema = secretValueSchema.refine(
  (value) => typeof value !== "string" || value.trim().length > 0,
  "QuestDB credential must not be empty",
);

const questDbUrlSchema = z.union([
  z.url("questdb.url must be an absolute URL"),
  secretPlaceholderSchema,
]);

export const projectExtrasSchema = z.object({
  archiver: z
    .object({
      legacySources: z
        .array(
          z.object({
            id: z.string().regex(/^[a-zA-Z0-9][a-zA-Z0-9-]{0,63}$/),
            directory: z.string().startsWith("/"),
          }),
        )
        .max(16)
        .optional()
        .describe(
          "Node-local allowlist of sealed legacy spool directories. Select by ID through the import API; never synchronize these host paths across controllers. Changes require archiver restart.",
        ),
      legacyImport: z
        .object({
          batchSize: z.number().int().min(1).max(512).optional(),
          concurrency: z
            .number()
            .int()
            .min(1)
            .max(128)
            .optional()
            .describe(
              "Requested legacy writers (default 64). Runtime caps this at floor(ingestQueueMaxEvents / 8), minimum 1. Requesting 128 requires at least 1024 live event capacity; effective limits are reported by import status.",
            ),
          maxFileBytes: z.number().int().min(1).max(16777216).optional(),
          maxBatchBytes: z.number().int().min(1).max(67108864).optional(),
          intervalMs: z
            .number()
            .int()
            .min(10)
            .max(60000)
            .optional()
            .describe(
              "Healthy-pass pacing (default 100 ms). Explicit 10 ms is opt-in for measured local capacity; failure backoff, queue limits and ACK checks remain unchanged.",
            ),
        })
        .optional()
        .describe(
          "Bounded legacy import settings, read at startup. Defaults: 256 events/pass, up to 64 writers (capped at one eighth of live event capacity), 1 MiB/file, 16 MiB/pass, 100 ms between passes. Source must be closed by its old writer before starting import.",
        ),
      inactiveBufferMax: z
        .number()
        .int()
        .positive()
        .optional()
        .describe(
          "Max number of events kept in-memory while waiting for the controller/GraphQL active-topics registry (default 2000). Overflow is discarded because inactive topics are not eligible for history.",
        ),
      inactiveBufferMaxAgeMs: z
        .number()
        .int()
        .positive()
        .optional()
        .describe(
          "Max age (ms) for buffered inactive-topic events before discarding them (default 300000 = 5min).",
        ),
      ingestQueueMaxEvents: z
        .number()
        .int()
        .positive()
        .optional()
        .describe(
          "Maximum active MQTT events retained in memory while QuestDB is being written (default 512). Excess events are immediately persisted to ./event_storage.",
        ),
      ingestQueueMaxBytes: z
        .number()
        .int()
        .positive()
        .optional()
        .describe(
          "Maximum combined payload size in bytes retained by the active ingest queue (default 16777216 = 16 MiB). Excess events are immediately persisted to ./event_storage.",
        ),
      ingestConcurrency: z
        .number()
        .int()
        .positive()
        .optional()
        .describe(
          "Maximum concurrent QuestDB ingest operations (default 512). A single worker waits for each shared batch flush and cannot keep up with sustained MQTT traffic.",
        ),
      storedReplayBatchSize: z
        .number()
        .int()
        .positive()
        .optional()
        .describe(
          "Maximum durable event-storage files handled in one fair replay pass (default 256). Replay keeps 25% of live ingest capacity reserved for new MQTT traffic.",
        ),
      storedReplayIntervalMs: z
        .number()
        .int()
        .min(250)
        .optional()
        .describe(
          "Delay in milliseconds between completed durable replay passes (default 500). Lower values drain backlogs faster but add QuestDB load.",
        ),
      identityEnrichmentEnabled: z
        .boolean()
        .optional()
        .describe(
          "Enrich QuestDB rows with controller-issued stable entity identity evidence. Disabled by default for a backward-compatible staged rollout.",
        ),
      identityResolutionRetryMaxAgeMs: z
        .number()
        .int()
        .positive()
        .optional()
        .describe(
          "Maximum time in milliseconds to retain an event while exact entity identity resolution is temporarily unavailable (default 30000). After this bound the event is written in legacy form.",
        ),
      traceIngest: z
        .boolean()
        .optional()
        .describe(
          "Enable ingest/buffer trace logs. Equivalent to setting UNS_ARCHIVER_TRACE=1 or UNS_ARCHIVER_TRACE_INGEST=1.",
        ),
    })
    .optional()
    .describe("Archiver runtime settings."),
  questdb: z
    .object({
      configurationString: z
        .string()
        .min(1, "questdb.configurationString must not be empty")
        .optional()
        .describe(
          "Legacy QuestDB ILP connection string. Prefer url, username, and password for production secrets.",
        ),
      url: questDbUrlSchema
        .optional()
        .describe(
          "QuestDB HTTP endpoint used with the structured credential form (for example https://questdb.example:9000).",
        ),
      username: nonEmptySecretValueSchema
        .optional()
        .describe(
          "QuestDB username used with questdb.url. Store as a secret reference in production.",
        ),
      password: nonEmptySecretValueSchema
        .optional()
        .describe(
          "QuestDB password used with questdb.url. Store as a secret reference in production.",
        ),
      batch: z
        .object({
          flushIntervalMs: z
            .number()
            .int()
            .positive()
            .optional()
            .describe(
              "Maximum time a completed ILP row waits before a shared QuestDB flush (default 1000ms).",
            ),
          maxRows: z
            .number()
            .int()
            .positive()
            .optional()
            .describe(
              "Number of ILP rows that trigger an immediate shared QuestDB flush (default 512).",
            ),
          maxPendingRows: z
            .number()
            .int()
            .positive()
            .optional()
            .describe(
              "Maximum accepted ILP rows across the queued and flushing shared sender batch (default 2048). A full queue rejects new writes so the archiver can persist them to event storage.",
            ),
        })
        .optional()
        .superRefine((value, context) => {
          if (
            value?.maxRows &&
            value?.maxPendingRows &&
            value.maxPendingRows < value.maxRows
          ) {
            context.addIssue({
              code: "custom",
              message:
                "questdb.batch.maxPendingRows must be greater than or equal to questdb.batch.maxRows.",
            });
          }
        })
        .describe("Bounded shared QuestDB ILP batching settings."),
      dataStorage: z
        .array(
          z.object({
            tablePrefix: z
              .string()
              .min(1, "questdb.dataStorage[].tablePrefix is required")
              .describe(
                "Prefix used when naming QuestDB tables for this topic",
              ),
            topic: z
              .string()
              .min(1, "questdb.dataStorage[].topic is required")
              .describe("UNS topic filter subscribed for ingestion"),
            ingestMode: z
              .enum(["append", "dedup", "window_replace"])
              .optional()
              .describe(
                "Default ingestion mode: append (default), dedup (upsert on eventId), or window_replace. Note: QuestDB has no row-level DELETE; window_replace is implemented as soft delete (sets deleted=true for rows in the window that were not refreshed).",
              ),
            ingestModeData: z
              .enum(["append", "dedup", "window_replace"])
              .optional()
              .describe("Optional override of ingestMode for data messages"),
            ingestModeTable: z
              .enum(["append", "dedup", "window_replace"])
              .optional()
              .describe("Optional override of ingestMode for table messages"),
            dataGroups: z
              .array(
                z.object({
                  name: z
                    .string()
                    .min(
                      1,
                      "questdb.dataStorage[].dataGroups[].name is required",
                    ),
                  ingestMode: z
                    .enum(["append", "dedup", "window_replace"])
                    .describe("Ingestion mode override for this dataGroup"),
                  ingestModeData: z
                    .enum(["append", "dedup", "window_replace"])
                    .optional()
                    .describe(
                      "Optional override for data messages in this dataGroup",
                    ),
                  ingestModeTable: z
                    .enum(["append", "dedup", "window_replace"])
                    .optional()
                    .describe(
                      "Optional override for table messages in this dataGroup",
                    ),
                }),
              )
              .optional()
              .describe("Per-dataGroup ingestion mode overrides"),
          }),
        )
        .min(1, "At least one QuestDB data storage target is required"),
    })
    .superRefine((value, context) => {
      if (value.configurationString) {
        if (value.url || value.username || value.password) {
          context.addIssue({
            code: "custom",
            message:
              "Use either questdb.configurationString or questdb.url with username and password, not both.",
          });
        }
        return;
      }
      if (!value.url || !value.username || !value.password) {
        context.addIssue({
          code: "custom",
          message:
            "QuestDB requires configurationString or url, username, and password.",
        });
      }
    }),
});

export type ProjectExtras = z.infer<typeof projectExtrasSchema>;
