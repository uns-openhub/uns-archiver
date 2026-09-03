import type { ArchiverEntityBindingResolution } from "./entity-binding-client.js";
import type { QuestDbEntityIdentityEvidence } from "./writers/questDbWriter.js";

export type EntityIdentityEvidenceDecision =
  | { status: "resolved"; evidence: QuestDbEntityIdentityEvidence }
  | { status: "unresolved" | "ambiguous" | "inexact" | "invalid"; evidence: null };

function normalizedTopic(value: string): string {
  return value.trim().normalize("NFC").replace(/^\/+|\/+$/g, "");
}

export function selectExactEntityIdentityEvidence(
  requestedTopic: string,
  asOf: string,
  resolution: ArchiverEntityBindingResolution | null,
): EntityIdentityEvidenceDecision {
  if (!resolution) return { status: "unresolved", evidence: null };
  if (resolution.status === "ambiguous") return { status: "ambiguous", evidence: null };
  if (resolution.status !== "resolved") return { status: "unresolved", evidence: null };
  if (
    resolution.bindingKind !== "attribute-topic"
    || !resolution.matchedPath
    || normalizedTopic(resolution.matchedPath) !== normalizedTopic(requestedTopic)
  ) return { status: "inexact", evidence: null };
  const instant = new Date(asOf).getTime();
  const validFrom = resolution.validFrom ? new Date(resolution.validFrom).getTime() : Number.NaN;
  const validTo = resolution.validTo ? new Date(resolution.validTo).getTime() : null;
  if (
    !Number.isFinite(instant)
    || !Number.isFinite(validFrom)
    || instant < validFrom
    || (validTo !== null && (!Number.isFinite(validTo) || instant >= validTo))
    || !resolution.stableEntityId
    || !resolution.entityTypeKey
    || !resolution.timeBasis
    || !resolution.revision
    || !resolution.digest
  ) return { status: "invalid", evidence: null };
  return {
    status: "resolved",
    evidence: {
      stableEntityId: resolution.stableEntityId,
      entityTypeKey: resolution.entityTypeKey,
      bindingRevision: resolution.revision,
      bindingDigest: resolution.digest,
      resolution: "resolved",
      timeBasis: resolution.timeBasis,
    },
  };
}
