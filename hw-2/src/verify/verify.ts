import { Kafka, logLevel } from "kafkajs";
import { makePool } from "../shared/postgres.js";
import { captureDltBounds, scanDlt, assertRetained } from "./dlt-scan.js";
import { readVerificationSnapshot } from "./verify-store.js";
import { readVerifyConfig } from "./config.js";
import { analyzeEvents, formatReport } from "./verification.js";

function makeVerifyResources(config: ReturnType<typeof readVerifyConfig>) {
  const kafka = new Kafka({
    clientId: "events-verify",
    brokers: config.brokers,
    connectionTimeout: 5000,
    requestTimeout: 10000,
    retry: { retries: 0 },
    logCreator:
      () =>
      ({ level, log }) => {
        if (level <= logLevel.WARN) console.error(`Kafka: ${log.message}`);
      },
  });
  const admin = kafka.admin();
  const pool = makePool(config.databaseUrl);
  pool.on("error", (error) => console.error(`PostgreSQL: ${error.message}`));
  return { kafka, admin, pool };
}
export async function verify(
  config: ReturnType<typeof readVerifyConfig>,
  signal?: AbortSignal,
  resources = makeVerifyResources(config),
) {
  const { kafka, admin, pool } = resources;
  try {
    signal?.throwIfAborted();
    await admin.connect();
    const bounds = await captureDltBounds(admin, config.topic);
    const snapshot = await readVerificationSnapshot(pool);
    const dlt = await scanDlt(kafka, config.topic, bounds, {
      timeoutMs: config.timeoutMs,
      signal,
      diagnostic: (message) => console.error(message),
    });
    assertRetained(bounds, await captureDltBounds(admin, config.topic));
    signal?.throwIfAborted();
    return {
      result: analyzeEvents(snapshot.events, dlt.events, snapshot.duplicates),
      skipped: dlt.skipped,
    };
  } finally {
    const closed = await Promise.allSettled([admin.disconnect(), pool.end()]);
    const failure = closed.find((r) => r.status === "rejected");
    if (failure?.status === "rejected") throw failure.reason;
  }
}

export async function runVerify(
  env: NodeJS.ProcessEnv = process.env,
  signal?: AbortSignal,
  resources?: Parameters<typeof verify>[2],
) {
  const config = readVerifyConfig(env);
  const checkedAt = new Date().toISOString();
  // Print identity before connecting; stdout remains identifiable even when IO fails.
  console.log(`github: ${config.handle} | checked_at: ${checkedAt}`);
  const { result, skipped } = await verify(config, signal, resources);
  console.log(
    formatReport(config.handle, checkedAt, result, skipped)
      .split("\n")
      .slice(1)
      .join("\n"),
  );
}
