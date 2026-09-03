import type { ArchiverEntityBindingResolution } from "./entity-binding-client.js";
import { selectExactEntityIdentityEvidence } from "./entity-identity-evidence.js";
import type { QuestDbEntityIdentityEvidence } from "./writers/questDbWriter.js";

export type PersistedEntityResolutionEnvelope =
  | {
      schemaVersion: 1;
      status: "resolved";
      topic: string;
      eventTime: string;
      validFrom: string;
      validTo: string | null;
      evidence: QuestDbEntityIdentityEvidence;
    }
  | {
      schemaVersion: 1;
      status: "retry";
      topic: string;
      eventTime: string;
      firstAttemptAt: string;
    };

export type EntityResolutionPolicyDecision =
  | {
      action: "enriched-write";
      evidence: QuestDbEntityIdentityEvidence;
      envelope: PersistedEntityResolutionEnvelope;
    }
  | {
      action: "legacy-write";
      reason: "unresolved" | "ambiguous" | "inexact" | "invalid";
      evidence: null;
      envelope: null;
    }
  | {
      action: "retry";
      reason: "controller-unavailable";
      evidence: null;
      envelope: PersistedEntityResolutionEnvelope;
    }
  | {
      action: "legacy-write";
      reason: "retry-expired";
      evidence: null;
      envelope: null;
    };

function normalizeTopic(value: string): string {
  return value
    .trim()
    .normalize("NFC")
    .replace(/^\/+|\/+$/g, "");
}

function timestamp(value: string): number {
  return new Date(value).getTime();
}

export function resolvedEnvelopeEvidence(
  envelope: unknown,
  topicValue: string,
  eventTimeValue: string,
): QuestDbEntityIdentityEvidence | null {
  if (!envelope || typeof envelope !== "object") return null;
  const value = envelope as Partial<PersistedEntityResolutionEnvelope>;
  if (
    value.schemaVersion !== 1 ||
    value.status !== "resolved" ||
    normalizeTopic(String(value.topic ?? "")) !== normalizeTopic(topicValue) ||
    value.eventTime !== eventTimeValue ||
    typeof value.validFrom !== "string" ||
    !(typeof value.validTo === "string" || value.validTo === null) ||
    !value.evidence
  )
    return null;
  const eventTime = timestamp(eventTimeValue);
  const validFrom = timestamp(value.validFrom);
  const validTo = value.validTo === null ? null : timestamp(value.validTo);
  if (
    !Number.isFinite(eventTime) ||
    !Number.isFinite(validFrom) ||
    eventTime < validFrom ||
    (validTo !== null && (!Number.isFinite(validTo) || eventTime >= validTo))
  )
    return null;
  return value.evidence;
}

export function decideEntityResolution(
  input: {
    topic: string;
    eventTime: string;
    resolution?: ArchiverEntityBindingResolution | null;
    controllerError?: unknown;
    persistedEnvelope?: unknown;
  },
  options: { now: string; retryMaxAgeMs: number },
): EntityResolutionPolicyDecision {
  const persistedEvidence = resolvedEnvelopeEvidence(
    input.persistedEnvelope,
    input.topic,
    input.eventTime,
  );
  if (persistedEvidence) {
    return {
      action: "enriched-write",
      evidence: persistedEvidence,
      envelope: input.persistedEnvelope as PersistedEntityResolutionEnvelope,
    };
  }
  if (!Number.isFinite(options.retryMaxAgeMs) || options.retryMaxAgeMs < 0) {
    throw new RangeError("retryMaxAgeMs must be a non-negative finite number");
  }
  if (input.controllerError !== undefined) {
    const previous =
      input.persistedEnvelope && typeof input.persistedEnvelope === "object"
        ? (input.persistedEnvelope as Partial<PersistedEntityResolutionEnvelope>)
        : null;
    const firstAttemptAt =
      previous?.status === "retry" &&
      typeof previous.firstAttemptAt === "string"
        ? previous.firstAttemptAt
        : options.now;
    const ageMs = timestamp(options.now) - timestamp(firstAttemptAt);
    if (Number.isFinite(ageMs) && ageMs >= options.retryMaxAgeMs) {
      return {
        action: "legacy-write",
        reason: "retry-expired",
        evidence: null,
        envelope: null,
      };
    }
    return {
      action: "retry",
      reason: "controller-unavailable",
      evidence: null,
      envelope: {
        schemaVersion: 1,
        status: "retry",
        topic: normalizeTopic(input.topic),
        eventTime: input.eventTime,
        firstAttemptAt,
      },
    };
  }
  const selected = selectExactEntityIdentityEvidence(
    input.topic,
    input.eventTime,
    input.resolution ?? null,
  );
  if (selected.status !== "resolved") {
    return {
      action: "legacy-write",
      reason: selected.status,
      evidence: null,
      envelope: null,
    };
  }
  const resolution = input.resolution!;
  return {
    action: "enriched-write",
    evidence: selected.evidence,
    envelope: {
      schemaVersion: 1,
      status: "resolved",
      topic: normalizeTopic(input.topic),
      eventTime: input.eventTime,
      validFrom: resolution.validFrom!,
      validTo: resolution.validTo,
      evidence: selected.evidence,
    },
  };
}
