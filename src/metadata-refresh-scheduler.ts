/** Coalesces unknown-topic observations. One bounded refresh every five seconds
 * bridges registration races without a GraphQL request per MQTT packet. */
export function metadataRefreshScheduler(refresh: () => Promise<void>, intervalMs = 5_000) {
  let timer: ReturnType<typeof setTimeout> | undefined;
  let running = false;
  let closed = false;
  let lastStarted = -Infinity;
  return {
    request(): void {
      if (closed || timer || running) return;
      timer = setTimeout(() => {
        timer = undefined;
        if (closed) return;
        lastStarted = Date.now();
        running = true;
        void refresh().catch(() => undefined).finally(() => { running = false; });
      }, Math.max(0, lastStarted + intervalMs - Date.now()));
      timer.unref?.();
    },
    close(): void { closed = true; clearTimeout(timer); timer = undefined; },
  };
}
