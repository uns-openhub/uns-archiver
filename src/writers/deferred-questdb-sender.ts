import { Sender } from "@questdb/nodejs-client";

/** Shared batcher owns flush boundaries. Keep protocol negotiation lazy so a
 * stopped database cannot prevent MQTT intake and durable storage at startup. */
export function deferredQuestDbSender(
  configuration: string,
): Sender & { prepare(): Promise<void> } {
  let sender: Sender | undefined;
  let creating: Promise<void> | undefined;
  let closed = false;
  let retryAfter = 0;
  const parts = configuration
    .split(";")
    .filter((part) => part && !part.startsWith("auto_flush="));
  parts.push("auto_flush=off");
  const prepare = async () => {
    if (closed) throw new Error("QuestDB sender closed");
    if (sender) return;
    if (creating) return creating;
    if (Date.now() < retryAfter)
      throw new Error("QuestDB negotiation retry pending");
    creating = Sender.fromConfig(parts.join(";") + ";")
      .then((value) => {
        sender = value;
      })
      .catch(() => {
        retryAfter = Date.now() + 1000;
        throw new Error("QuestDB negotiation unavailable; event retained");
      })
      .finally(() => {
        creating = undefined;
      });
    return creating;
  };
  const delegated = {
    prepare,
    table(name: string) {
      if (!sender) throw new Error("QuestDB sender not prepared");
      return sender.table(name);
    },
    reset() {
      sender?.reset();
      return delegated;
    },
    async flush() {
      if (!sender) throw new Error("QuestDB sender not prepared");
      return sender.flush();
    },
    async close() {
      closed = true;
      await creating?.catch(() => undefined);
      await sender?.close();
    },
  };
  return delegated as unknown as Sender & { prepare(): Promise<void> };
}
