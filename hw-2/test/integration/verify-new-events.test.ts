import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { spawn, execFile } from "node:child_process";
import { promisify } from "node:util";
import { setTimeout as delay } from "node:timers/promises";
import { Kafka, logLevel } from "kafkajs";
import { Pool } from "pg";
import { createGenerator } from "../../src/events.js";

const exec = promisify(execFile);

async function until(check: () => Promise<boolean>, description: string) {
  const deadline = performance.now() + 15000;
  while (performance.now() < deadline) {
    if (await check()) return;
    await delay(25);
  }
  throw Error(`Timed out waiting for ${description}`);
}

function metric(report: string, name: string) {
  const match = report.match(new RegExp(`^${name}: (.+)$`, "m"));
  assert.ok(match, `Missing ${name} in report`);
  return match[1];
}

test("new Kafka events acquire processing_order; verify reports complete then partial with legacy NULL", { timeout: 60000 }, async (t) => {
  assert.ok(process.env.DATABASE_URL, "DATABASE_URL is required");
  assert.ok(process.env.KAFKA_BROKERS, "KAFKA_BROKERS is required");
  const id = randomUUID().replaceAll("-", "");
  const schema = `verify_e2e_${id}`;
  const topic = `verify-e2e-${id}`;
  const dlt = `${topic}.dlt`;
  const group = `verify-e2e-${id}`;
  const kafka = new Kafka({
    clientId: group, brokers: process.env.KAFKA_BROKERS.split(","),
    logLevel: logLevel.ERROR, retry: { retries: 3 },
    connectionTimeout: 5000, requestTimeout: 10000,
  });
  const admin = kafka.admin();
  const producer = kafka.producer();
  const database = new Pool({ connectionString: process.env.DATABASE_URL });
  const isolatedUrl = new URL(process.env.DATABASE_URL);
  isolatedUrl.searchParams.set("options", `-c search_path=${schema}`);
  const pool = new Pool({ connectionString: isolatedUrl.toString(), statement_timeout: 5000 });
  const env = {
    ...process.env, DATABASE_URL: isolatedUrl.toString(),
    KAFKA_TOPIC: topic, KAFKA_DLT_TOPIC: dlt, KAFKA_GROUP_ID: group,
    CONSUMER_ID: "consumer-1", PROCESSING_DELAY_MS: "0", RETRY_DELAY_MS: "10",
    GITHUB_HANDLE: "example", VERIFY_TIMEOUT_MS: "15000",
  };
  let consumer: ReturnType<typeof spawn> | undefined;
  let stopped: Promise<{ code: number | null; signal: NodeJS.Signals | null }> | undefined;
  let consumerError = "";
  let createdSchema = false;
  let createdTopics = false;
  try {
    await database.query(`CREATE SCHEMA ${schema}`);
    createdSchema = true;
    await pool.query(await readFile("postgres/init.sql", "utf8"));
    await admin.connect();
    const workingOffsets = await admin.fetchOffsets({ groupId: "events-processor", topics: ["events", "events.dlt"] });
    await admin.createTopics({ timeout: 10000, waitForLeaders: true,
      topics: [topic, dlt].map(topic => ({ topic, numPartitions: 3, replicationFactor: 1 })) });
    createdTopics = true;
    consumer = spawn(process.execPath, ["dist/src/consumer-main.js"], { env, stdio: ["ignore", "pipe", "pipe"] });
    consumer.stdout!.resume();
    consumer.stderr!.on("data", chunk => { consumerError += String(chunk); });
    stopped = new Promise((resolve, reject) => {
      consumer!.once("error", reject);
      consumer!.once("exit", (code, signal) => resolve({ code, signal }));
    });
    void stopped.catch(() => {});
    const generate = createGenerator(["new-user"], 0);
    const events = [generate()!, generate()!, generate()!];
    const poison = { ...generate()!, payload: { poison: true } };
    await producer.connect();
    await producer.send({ topic, acks: -1,
      messages: [...events, events[1]!, poison].map(e => ({ partition: 0, key: e.user_id, value: JSON.stringify(e) })) });
    await until(async () => {
      assert.equal(consumer!.exitCode, null, consumerError);
      const result = await pool.query("SELECT count(*)::int AS n FROM processed_events");
      const stats = await pool.query("SELECT COALESCE(SUM(duplicates),0)::text AS n FROM consumer_stats");
      const offsets = await admin.fetchOffsets({ groupId: group, topics: [topic] });
      return result.rows[0].n === 3 && stats.rows[0].n === "1" &&
        offsets[0]!.partitions.find(p => p.partition === 0)?.offset === "5";
    }, "three committed new events, one duplicate and confirmed DLT delivery");
    const persisted = await pool.query("SELECT seq, processing_order FROM processed_events ORDER BY processing_order");
    assert.deepEqual(persisted.rows, [
      { seq: "1", processing_order: "1" },
      { seq: "2", processing_order: "2" },
      { seq: "3", processing_order: "3" },
    ]);
    const check = async () => {
      const { stdout, stderr } = await exec(process.execPath, ["--trace-warnings", "dist/src/verify-main.js"], { env, timeout: 25000 });
      assert.doesNotMatch(stderr, /TimeoutNegativeWarning/);
      assert.match(stdout, /^github: example \| checked_at: /);
      return stdout;
    };
    const fresh = await check();
    assert.equal(metric(fresh, "ordered_events_checked"), "3");
    assert.equal(metric(fresh, "unknown_order_events"), "0");
    assert.equal(metric(fresh, "ordering_status"), "complete");
    assert.equal(metric(fresh, "dlt_events"), "1");
    assert.equal(metric(fresh, "duplicates"), "1");
    assert.equal(metric(fresh, "gaps"), "0");
    assert.equal(metric(fresh, "ordering_violations"), "0");
    t.diagnostic("Fresh events: processing_order=[1,2,3], ordered_events_checked=3, unknown_order_events=0, ordering_status=complete");

    // Simulate one historical NULL inside this newly created test schema only.
    const legacy = createGenerator(["legacy-user"], 0)()!;
    await pool.query(`INSERT INTO processed_events
      (event_id,user_id,type,seq,produced_at,payload,consumer_id,topic,partition,"offset")
      VALUES($1,$2,$3,$4,$5,$6,'consumer-1',$7,0,0)`,
      [legacy.event_id, legacy.user_id, legacy.type, legacy.seq, legacy.produced_at, legacy.payload, topic]);
    const mixed = await check();
    assert.equal(metric(mixed, "processed_events"), "4");
    assert.equal(metric(mixed, "ordered_events_checked"), "3");
    assert.equal(metric(mixed, "unknown_order_events"), "1");
    assert.equal(metric(mixed, "ordering_status"), "partial");
    assert.doesNotMatch(consumerError, /TimeoutNegativeWarning/);
    assert.deepEqual(await admin.fetchOffsets({ groupId: "events-processor", topics: ["events", "events.dlt"] }), workingOffsets);
    t.diagnostic("Mixed history: ordered_events_checked=3, unknown_order_events=1, ordering_status=partial; events-processor offsets unchanged");
  } finally {
    if (consumer && stopped) {
      consumer.kill("SIGTERM");
      const deadline = setTimeout(() => consumer!.kill("SIGKILL"), 10000);
      try { assert.deepEqual(await stopped, { code: 0, signal: null }, consumerError); }
      finally { clearTimeout(deadline); }
    }
    await producer.disconnect();
    await pool.end();
    // Cleanup is restricted to resources created by this test; existing data is untouched.
    try {
      if (createdTopics) await admin.deleteTopics({ topics: [topic, dlt], timeout: 10000 });
    } finally {
      await admin.disconnect();
      try { if (createdSchema) await database.query(`DROP SCHEMA ${schema} CASCADE`); }
      finally { await database.end(); }
    }
  }
});
