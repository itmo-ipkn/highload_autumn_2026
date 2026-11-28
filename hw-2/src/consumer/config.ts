import {
  validateBrokers,
  validateTopic,
  validateConsumerId,
  validateDltTopic,
  validateGroupId,
  validateDatabaseUrl,
  parseInteger,
} from "../shared/config-validation.js";

export function readConsumerConfig(env: NodeJS.ProcessEnv = process.env) {
  const consumerId = env.CONSUMER_ID ?? "consumer-1";
  validateConsumerId(consumerId);

  const brokers = (
    env.KAFKA_BROKERS ?? "localhost:19092,localhost:29092,localhost:39092"
  )
    .split(",")
    .map((broker) => broker.trim());
  validateBrokers(brokers);

  const topic = env.KAFKA_TOPIC ?? "events";
  const dltTopic = env.KAFKA_DLT_TOPIC ?? "events.dlt";
  validateTopic(topic);
  validateDltTopic(topic, dltTopic);

  const groupId = env.KAFKA_GROUP_ID ?? "events-processor";
  validateGroupId(groupId);

  const databaseUrl = env.DATABASE_URL;
  validateDatabaseUrl(databaseUrl);

  return {
    consumerId,
    brokers,
    topic,
    dltTopic,
    groupId,
    databaseUrl,
    processingDelayMs: parseInteger(
      env.PROCESSING_DELAY_MS ?? "100", "PROCESSING_DELAY_MS", 0,
    ),
    maxAttempts: parseInteger(
      env.MAX_PROCESSING_ATTEMPTS ?? "3", "MAX_PROCESSING_ATTEMPTS", 1, 3,
    ),
    retryDelayMs: parseInteger(
      env.RETRY_DELAY_MS ?? "300", "RETRY_DELAY_MS", 1,
    ),
  };
}

export type ConsumerConfig = ReturnType<typeof readConsumerConfig>;
