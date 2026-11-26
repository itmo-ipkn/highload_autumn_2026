export function validateProducerId(producerId: string) {
  if (!/^producer-[123]$/.test(producerId)) {
    throw new Error("PRODUCER_ID must be producer-1..producer-3");
  }
}

export function validateBrokers(brokers: string[]) {
  const hasInvalidAddress = brokers.some((broker) => {
    const port = Number(broker.slice(broker.lastIndexOf(":") + 1));
    return !/^\S+:\d+$/.test(broker) || port < 1 || port > 65535;
  });

  if (hasInvalidAddress) {
    throw new Error("KAFKA_BROKERS must contain host:port addresses");
  }
}

export function validateTopic(topic: string) {
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

export function validateKeyMode(
  keyMode: string,
): asserts keyMode is "user_id" | "none" {
  if (keyMode !== "user_id" && keyMode !== "none") {
    throw new Error("KEY_MODE must be user_id or none");
  }
}

export function validatePositiveInteger(value: number, name: string) {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new Error(`${name} must be a positive safe integer`);
  }
}

export function validateInterval(intervalMs: number) {
  validatePositiveInteger(intervalMs, "EVENT_INTERVAL_MS");
  if (intervalMs > 2147483647) {
    throw new Error("EVENT_INTERVAL_MS exceeds Node timer limit");
  }
}

export function validatePoisonRate(poisonRate: number, rawValue: string | undefined) {
  if (
    rawValue?.trim() === "" ||
    !Number.isFinite(poisonRate) ||
    poisonRate < 0 ||
    poisonRate > 1
  ) {
    throw new Error("POISON_RATE must be between 0 and 1");
  }
}

export function validateConsumerId(consumerId: string) {
  if (!/^consumer-[123]$/.test(consumerId)) {
    throw new Error("CONSUMER_ID must be consumer-1..consumer-3");
  }
}

export function validateDltTopic(topic: string, dltTopic: string) {
  validateTopic(dltTopic);
  if (topic === dltTopic) {
    throw new Error("KAFKA_DLT_TOPIC must differ from KAFKA_TOPIC");
  }
}

export function validateGroupId(groupId: string) {
  if (!groupId.trim() || groupId !== groupId.trim()) {
    throw new Error("Invalid KAFKA_GROUP_ID");
  }
}

export function validateDatabaseUrl(
  databaseUrl: string | undefined,
): asserts databaseUrl is string {
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required");
  }

  const url = new URL(databaseUrl);
  if (
    !["postgres:", "postgresql:"].includes(url.protocol) ||
    !url.hostname ||
    url.pathname.length < 2
  ) {
    throw new Error("Invalid DATABASE_URL");
  }
}

export function parseInteger(
  raw: string,
  name: string,
  min: number,
  max = 2147483647,
) {
  const value = Number(raw);
  if (
    !raw.trim() ||
    !Number.isSafeInteger(value) ||
    value < min ||
    value > max
  ) {
    throw new Error(`Invalid ${name}`);
  }
  return value;
}
