import { z } from "zod";
import { TopicMatcher } from "./topic-matcher.js";

export const archiveTopicExclusionsSchema = z
  .array(
    z
      .string()
      .min(1)
      .max(65535)
      .regex(
        /^(?![!])(?:[^+#\0/]*|\+)(?:\/(?:[^+#\0/]*|\+))*(?:\/#)?$|^#$/,
        "Use a plain MQTT filter with whole-level + or final # wildcards, without a ! prefix",
      ),
  )
  .max(128);

/** Applies only before accepting a new MQTT occurrence. Never use this to
 * filter an accepted queue, journal replay or a retained legacy source. */
export class ArchiveTopicAdmission {
  private filters: string[] = [];
  private excludedEvents = 0;
  update(filters: unknown = []): void {
    this.filters = [...new Set(archiveTopicExclusionsSchema.parse(filters))];
  }
  accepts(topic: string): boolean {
    if (!this.filters.some((filter) => TopicMatcher.matches(filter, topic)))
      return true;
    this.excludedEvents++;
    return false;
  }
  snapshot() {
    return {
      excludeTopics: [...this.filters],
      excludedEvents: this.excludedEvents,
    };
  }
}
