export type ArchiverShutdownSteps = {
  stopMqtt: () => Promise<void>;
  waitForLiveIngest: () => Promise<void>;
  waitForStoredReplay: () => Promise<void>;
  closeQuestDb: () => Promise<void>;
};

/**
 * Preserve durability boundaries during shutdown: no new MQTT input, then
 * live work, durable replay work, and finally the shared QuestDB sender.
 */
export const drainArchiverForShutdown = async (
  steps: ArchiverShutdownSteps,
): Promise<void> => {
  await steps.stopMqtt();
  await steps.waitForLiveIngest();
  await steps.waitForStoredReplay();
  await steps.closeQuestDb();
};
