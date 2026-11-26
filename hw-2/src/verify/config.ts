import {
  validateBrokers,
  validateTopic,
  validateDatabaseUrl,
  parseInteger,
} from "../shared/config-validation.js";
export function readVerifyConfig(env: NodeJS.ProcessEnv = process.env) {
  const handle = env.GITHUB_HANDLE;
  if (
    !handle ||
    !/^[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,37}[a-zA-Z0-9])?$/.test(handle)
  ) {
    throw Error("GITHUB_HANDLE is required and must be a valid GitHub handle");
  }
  const brokers = (
    env.KAFKA_BROKERS ?? "localhost:19092,localhost:29092,localhost:39092"
  )
    .split(",")
    .map((s) => s.trim());
  validateBrokers(brokers);
  const topic = env.KAFKA_DLT_TOPIC ?? "events.dlt";
  validateTopic(topic);
  const databaseUrl = env.DATABASE_URL;
  validateDatabaseUrl(databaseUrl);
  return {
    handle,
    brokers,
    topic,
    databaseUrl,
    timeoutMs: parseInteger(
      env.VERIFY_TIMEOUT_MS ?? "60000",
      "VERIFY_TIMEOUT_MS",
      1,
    ),
  };
}
