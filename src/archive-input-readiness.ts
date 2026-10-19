export const INPUT_OBSERVATION_MAX_AGE_MS = 25_000;
export type ArchiveInputEvidence = {
  inputInstalled: boolean;
  topicsReady: boolean;
  topicsAuthoritative: boolean;
  subscriptionsConfirmed: boolean | null;
  observedAt: number | null;
  paused: boolean;
  stopping: boolean;
};
/** Admission evidence is separate from processing, batching and successful database writes. */
export function archiveInputReadiness(e: ArchiveInputEvidence, now = Date.now()) {
  const current = e.observedAt !== null && now - e.observedAt <= INPUT_OBSERVATION_MAX_AGE_MS && e.observedAt <= now + 5000;
  const ready = !e.stopping && !e.paused && e.inputInstalled && e.topicsReady && e.topicsAuthoritative && current && e.subscriptionsConfirmed === true;
  const phase = e.stopping ? 'stopping' : e.paused ? 'paused' : !e.inputInstalled || !e.topicsReady ? 'starting'
    : !e.topicsAuthoritative ? 'registry-unconfirmed' : !current || e.subscriptionsConfirmed === null ? 'unconfirmed'
    : !e.subscriptionsConfirmed ? 'waiting-for-subscriptions' : 'ready';
  const message = ready
    ? 'Ready to receive: MQTT subscriptions are acknowledged and the input handler and active-topic registry are prepared. Processing and database write acknowledgements are separate checks.'
    : e.stopping ? 'Input is stopping; do not start a source.' : e.paused ? 'Archive ingestion is paused; do not start a source.'
    : 'Not ready to receive: process Start is not confirmation of MQTT subscriptions and input preparation. Non-retained messages published before subscription can be missed.';
  return {id:'archiver-input-readiness',label:'Archive input',observationMode:'runtime-status',phase,ready,
    state:ready?'healthy':'degraded',healthy:ready,checkedAt:new Date(ready && e.observedAt !== null ? e.observedAt : now).toISOString(),message,
    ...(!ready?{action:'Wait for “Ready to receive” in service diagnostics before starting the source. Check the MQTT connection and active-topic registry if it remains unconfirmed.'}:{})};
}
