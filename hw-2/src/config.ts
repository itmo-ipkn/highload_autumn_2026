function validateProducerId(producerId: string) {
  if (!/^producer-[123]$/.test(producerId)) {
    throw new Error("PRODUCER_ID must be producer-1..producer-3");
  }
}

function validateBrokers(brokers: string[]) {
  const hasInvalidAddress = brokers.some((broker) => {
    const port = Number(broker.slice(broker.lastIndexOf(":") + 1));
    return !/^\S+:\d+$/.test(broker) || port < 1 || port > 65535;
  });

  if (hasInvalidAddress) {
    throw new Error("KAFKA_BROKERS must contain host:port addresses");
  }
}

function validateTopic(topic: string) {
  if (
    !topic ||
    !/^[a-zA-Z0-9._-]+$/.test(topic) ||
    topic === "." ||
    topic === ".." ||
    topic.length > 249
  ) {
    throw new Error("Invalid KAFKA_TOPIC");
  }
}

function validateKeyMode(
  keyMode: string,
): asserts keyMode is "user_id" | "none" {
  if (keyMode !== "user_id" && keyMode !== "none") {
    throw new Error("KEY_MODE must be user_id or none");
  }
}

function validatePositiveInteger(value: number, name: string) {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new Error(`${name} must be a positive safe integer`);
  }
}

function validateInterval(intervalMs: number) {
  validatePositiveInteger(intervalMs, "EVENT_INTERVAL_MS");
  if (intervalMs > 2147483647) {
    throw new Error("EVENT_INTERVAL_MS exceeds Node timer limit");
  }
}

function validatePoisonRate(poisonRate: number, rawValue: string | undefined) {
  if (
    rawValue?.trim() === "" ||
    !Number.isFinite(poisonRate) ||
    poisonRate < 0 ||
    poisonRate > 1
  ) {
    throw new Error("POISON_RATE must be between 0 and 1");
  }
}

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
