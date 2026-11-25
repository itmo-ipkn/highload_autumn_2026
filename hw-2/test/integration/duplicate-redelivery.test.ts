import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { Kafka, logLevel, type Consumer } from "kafkajs";
import { Pool } from "pg";
import { createGenerator } from "../../src/events.js";
import { handleBatch } from "../../src/consumer.js";
import { readConsumerConfig } from "../../src/consumer-config.js";
import { parseEvent } from "../../src/processor.js";
import { saveEvent } from "../../src/sink.js";

const exec = promisify(execFile);

function gate<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

function offsetFor(
  offsets: Awaited<ReturnType<ReturnType<Kafka["admin"]>["fetchOffsets"]>>,
  topic: string,
) {
  return offsets
    .find((entry) => entry.topic === topic)
    ?.partitions.find((partition) => partition.partition === 0)?.offset;
}

function metric(report: string, name: string) {
  const value = report.match(new RegExp(`^${name}: (.+)$`, "m"))?.[1];
  assert.ok(value, `Missing ${name} in verify output`);
  return value;
}

test(
  "uncommitted delivery is replayed, deduplicated and then committed",
  { timeout: 90000 },
  async () => {
    assert.ok(process.env.DATABASE_URL, "DATABASE_URL is required");
    assert.ok(process.env.KAFKA_BROKERS, "KAFKA_BROKERS is required");
    const id = randomUUID().replaceAll("-", "");
    const schema = `dedup_test_${id}`;
    const topic = `dedup-test-${id}`;
    const dltTopic = `${topic}.dlt`;
    const groupId = `dedup-test-${id}`;
    const database = new Pool({ connectionString: process.env.DATABASE_URL });
    const isolatedUrl = new URL(process.env.DATABASE_URL);
    isolatedUrl.searchParams.set("options", `-c search_path=${schema}`);
    const pool = new Pool({ connectionString: isolatedUrl.toString() });
    const kafka = new Kafka({
      clientId: groupId,
      brokers: process.env.KAFKA_BROKERS.split(","),
      logLevel: logLevel.ERROR,
      retry: { retries: 3 },
      connectionTimeout: 5000,
      requestTimeout: 10000,
    });
    const admin = kafka.admin();
    const producer = kafka.producer();
    const config = readConsumerConfig({
      DATABASE_URL: isolatedUrl.toString(),
      KAFKA_BROKERS: process.env.KAFKA_BROKERS,
      KAFKA_TOPIC: topic,
      KAFKA_DLT_TOPIC: dltTopic,
      KAFKA_GROUP_ID: groupId,
      CONSUMER_ID: "consumer-1",
      PROCESSING_DELAY_MS: "0",
      RETRY_DELAY_MS: "1",
    });
    const event = createGenerator([`user-${id}`], 0)()!;
    const consumers: Consumer[] = [];
    let schemaCreated = false;
    let topicsCreated = false;
    let adminConnected = false;
    let producerConnected = false;
    try {
      await database.query(`CREATE SCHEMA ${schema}`);
      schemaCreated = true;
      await pool.query(await readFile("postgres/init.sql", "utf8"));
      await admin.connect();
      adminConnected = true;
      await admin.createTopics({
        timeout: 10000,
        waitForLeaders: true,
        topics: [topic, dltTopic].map((name) => ({
          topic: name,
          numPartitions: 1,
          replicationFactor: 1,
        })),
      });
      topicsCreated = true;
      await producer.connect();
      producerConnected = true;
      await producer.send({
        topic,
        acks: -1,
        messages: [
          { partition: 0, key: event.user_id, value: JSON.stringify(event) },
        ],
      });

      const first = kafka.consumer({ groupId, allowAutoTopicCreation: false });
      consumers.push(first);
      await first.connect();
      await first.subscribe({ topic, fromBeginning: true });
      const firstDelivery = gate<string>();
      await first.run({
        autoCommit: false,
        eachBatchAutoResolve: false,
        eachBatch: async ({ batch }) => {
          for (const message of batch.messages) {
            assert.equal(parseEvent(message.value).event_id, event.event_id);
            assert.equal(
              await saveEvent(pool, event, {
                consumerId: "consumer-1",
                topic,
                partition: batch.partition,
                offset: message.offset,
              }),
              false,
            );
            firstDelivery.resolve(message.offset);
          }
        },
      });
      const originalOffset = await firstDelivery.promise;
      await first.stop();
      await first.disconnect();

      const initial = await pool.query(
        "SELECT event_id, processing_order FROM processed_events",
      );
      assert.deepEqual(initial.rows, [
        { event_id: event.event_id, processing_order: "1" },
      ]);
      assert.equal(
        (
          await pool.query(
            "SELECT COALESCE(SUM(duplicates),0)::text AS n FROM consumer_stats",
          )
        ).rows[0].n,
        "0",
      );
      assert.equal(
        offsetFor(
          await admin.fetchOffsets({ groupId, topics: [topic] }),
          topic,
        ),
        "-1",
      );

      const second = kafka.consumer({ groupId, allowAutoTopicCreation: false });
      consumers.push(second);
      await second.connect();
      await second.subscribe({ topic, fromBeginning: true });
      const repeatedDelivery = gate<string>();
      const logs: string[] = [];
      await second.run({
        autoCommit: false,
        eachBatchAutoResolve: false,
        eachBatch: async (payload) => {
          await handleBatch(
            config,
            payload,
            { stopping: false, expired: false },
            {
              save: (record, source) => saveEvent(pool, record, source),
              send: async () => {
                throw Error("Unexpected DLT send");
              },
              log: (line) => logs.push(line),
            },
          );
          repeatedDelivery.resolve(payload.batch.messages[0]!.offset);
        },
      });
      assert.equal(await repeatedDelivery.promise, originalOffset);
      await second.stop();
      await second.disconnect();

      assert.ok(
        logs.some((line) =>
          line.includes(`duplicate event_id=${event.event_id}`),
        ),
      );
      assert.deepEqual(
        (
          await pool.query(
            "SELECT event_id, processing_order FROM processed_events",
          )
        ).rows,
        initial.rows,
      );
      assert.equal(
        (
          await pool.query(
            "SELECT COALESCE(SUM(duplicates),0)::text AS n FROM consumer_stats",
          )
        ).rows[0].n,
        "1",
      );
      assert.equal(
        offsetFor(
          await admin.fetchOffsets({ groupId, topics: [topic] }),
          topic,
        ),
        (BigInt(originalOffset) + 1n).toString(),
      );

      const verifyEnv = {
        ...process.env,
        DATABASE_URL: isolatedUrl.toString(),
        KAFKA_DLT_TOPIC: dltTopic,
        GITHUB_HANDLE: "example",
        VERIFY_TIMEOUT_MS: "10000",
      };
      const { stdout } = await exec(
        process.execPath,
        ["dist/src/verify-main.js"],
        { env: verifyEnv, timeout: 20000 },
      );
      for (const [name, expected] of Object.entries({
        processed_events: "1",
        duplicates: "1",
        gaps: "0",
        ordering_violations: "0",
      })) {
        assert.equal(metric(stdout, name), expected);
      }
    } finally {
      for (const consumer of consumers.reverse()) await consumer.disconnect();
      if (producerConnected) await producer.disconnect();
      await pool.end();
      try {
        if (topicsCreated)
          await admin.deleteTopics({
            topics: [topic, dltTopic],
            timeout: 10000,
          });
      } finally {
        if (adminConnected) await admin.disconnect();
        try {
          if (schemaCreated)
            await database.query(`DROP SCHEMA ${schema} CASCADE`);
        } finally {
          await database.end();
        }
      }
    }
  },
);
