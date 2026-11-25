import {
  validateProducerId,
  validateBrokers,
  validateTopic,
  validateKeyMode,
  validateInterval,
  validatePositiveInteger,
  validatePoisonRate,
} from "./config-validation.js";

export function readConfig(env: NodeJS.ProcessEnv = process.env) {
  const producerId = env.PRODUCER_ID ?? "producer-1";

  validateProducerId(producerId);

  const brokers = (
    env.KAFKA_BROKERS ?? "localhost:19092,localhost:29092,localhost:39092"
  )
    .split(",")
    .map((broker) => broker.trim());

  validateBrokers(brokers);

  const topic = (env.KAFKA_TOPIC ?? "events").trim();

  validateTopic(topic);

  const keyMode = env.KEY_MODE ?? "user_id";

  validateKeyMode(keyMode);

  const intervalMs = Number(env.EVENT_INTERVAL_MS ?? 100);

  validateInterval(intervalMs);

  const userCount = Number(env.USER_COUNT ?? 30);

  validatePositiveInteger(userCount, "USER_COUNT");

  const poisonRate = Number(env.POISON_RATE ?? 0.02);

  validatePoisonRate(poisonRate, env.POISON_RATE);

  return {
    producerId,
    producerIndex: Number(producerId.at(-1)) - 1,
    brokers,
    topic,
    keyMode,
    intervalMs,
    userCount,
    poisonRate,
  };
}

export type Config = ReturnType<typeof readConfig>;
