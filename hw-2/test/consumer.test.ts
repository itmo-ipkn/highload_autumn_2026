import test from "node:test";
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import type { EachMessagePayload, KafkaMessage } from "kafkajs";
import type { Pool } from "pg";
import { readConsumerConfig } from "../src/consumer/config.js";
import { handleMessage, runConsumer, type MessageIO, type Resources } from "../src/consumer/consumer.js";
import { processRecord, type ProcessingIO } from "../src/consumer/processor.js";
import { saveEvent } from "../src/consumer/sink.js";
import { createGenerator } from "../src/producer/events.js";

const config = readConsumerConfig({ DATABASE_URL: "postgresql://test:test@localhost/test", PROCESSING_DELAY_MS: "0", RETRY_DELAY_MS: "1" });
const event = createGenerator(["user-1"], 0)()!;
const source = { consumerId: "consumer-1", topic: "events", partition: 1, offset: "9007199254740993" };
function message(poison = false): KafkaMessage {
  return { key: Buffer.from("user-1"), value: Buffer.from(JSON.stringify({ ...event, payload: { poison } })),
    timestamp: "0", attributes: 0, offset: source.offset, headers: { original: "header" } };
}
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(r => { resolve = r; });
  return { promise, resolve };
}
function fixture(m = message()) {
  const order: string[] = [];
  const state = { stopping: false, deadlineAt: Infinity };
  const payload: EachMessagePayload = {
    topic: source.topic, partition: source.partition, message: m,
    heartbeat: async () => { order.push("heartbeat"); }, pause: () => () => {},
  };
  const io: MessageIO = {
    save: async () => { order.push("save"); return false; },
    send: async () => { order.push("dlt"); return []; },
    commit: async offsets => { order.push(`commit:${offsets[0]!.offset}`); },
    log: () => {},
  };
  return { order, state, payload, io };
}

test("consumer configuration defaults and invalid values", () => {
  assert.equal(config.groupId, "events-processor"); assert.equal(config.maxAttempts, 3);
  for (const env of [{ DATABASE_URL: "" }, { DATABASE_URL: "https://host/db" }, { CONSUMER_ID: "x" },
    { KAFKA_BROKERS: "" }, { KAFKA_TOPIC: "" }, { KAFKA_DLT_TOPIC: "events" }, { KAFKA_GROUP_ID: " " },
    { PROCESSING_DELAY_MS: "" }, { PROCESSING_DELAY_MS: "-1" }, { PROCESSING_DELAY_MS: "2147483648" },
    { MAX_PROCESSING_ATTEMPTS: "4" }, { MAX_PROCESSING_ATTEMPTS: "0" }, { RETRY_DELAY_MS: "0" }]) {
    assert.throws(() => readConsumerConfig({ DATABASE_URL: config.databaseUrl, ...env }));
  }
});

test("successful message commits exact next offset after save", async () => {
  const f = fixture(); await handleMessage(config, f.payload, f.state, f.io);
  assert.ok(f.order.indexOf("save") < f.order.indexOf("commit:9007199254740994"));
  assert.equal(f.order.filter(x => x.startsWith("commit:")).length, 1);
});

for (const name of ["REBALANCE_IN_PROGRESS", "ILLEGAL_GENERATION", "UNKNOWN_MEMBER_ID", "request timeout"]) {
  test(`commit ${name} rejects the message`, async () => {
    const f = fixture(); f.io.commit = async () => { f.order.push("commit"); throw Error(name); };
    await assert.rejects(handleMessage(config, f.payload, f.state, f.io), new RegExp(name));
    assert.deepEqual(f.order.filter(x => x === "save" || x === "commit"), ["save", "commit"]);
  });
}

test("heartbeat failure during in-flight SQL prevents commit", async () => {
  const f = fixture(); const entered = deferred(); const release = deferred(); const lost = deferred(); let pulses = 0;
  f.io.save = async () => { entered.resolve(); await release.promise; return false; };
  f.payload.heartbeat = async () => { if (++pulses > 1) { await entered.promise; lost.resolve(); throw Error("UNKNOWN_MEMBER_ID"); } };
  const work = handleMessage(config, f.payload, f.state, f.io, 1);
  const rejected = assert.rejects(work, /UNKNOWN_MEMBER_ID/);
  await entered.promise; await lost.promise; release.resolve(); await rejected;
  assert.equal(f.order.some(x => x.startsWith("commit:")), false);
});

test("long processing keeps sending heartbeat", async () => {
  const f = fixture(); let pulses = 0; const entered = deferred(); const release = deferred();
  f.payload.heartbeat = async () => { pulses++; };
  f.io.save = async () => { entered.resolve(); await release.promise; return false; };
  const work = handleMessage(config, f.payload, f.state, f.io, 1);
  await entered.promise;
  await new Promise<void>(resolve => setTimeout(resolve, 15));
  assert.ok(pulses >= 2);
  release.resolve(); await work;
});

test("signal during SQL allows completed save and commit", async () => {
  const f = fixture(); const entered = deferred(); const release = deferred();
  f.io.save = async () => { f.order.push("save"); entered.resolve(); await release.promise; return false; };
  const work = handleMessage(config, f.payload, f.state, f.io);
  await entered.promise; f.state.stopping = true; release.resolve(); await work;
  assert.ok(f.order.includes("commit:9007199254740994"));
});

test("expired deadline prevents a new commit after completed SQL", async () => {
  const f = fixture(); f.io.save = async () => { f.state.deadlineAt = Date.now() - 1; return false; };
  await assert.rejects(handleMessage(config, f.payload, f.state, f.io), /deadline/);
  assert.equal(f.order.some(x => x.startsWith("commit:")), false);
});

test("stopping prevents a new message or infrastructure retry", async () => {
  const f = fixture(); f.state.stopping = true;
  await handleMessage(config, f.payload, f.state, f.io);
  assert.deepEqual(f.order, []);
  f.state.stopping = false;
  let saves = 0;
  f.io.save = async () => { saves++; f.state.stopping = true; throw Error("offline"); };
  await assert.rejects(handleMessage(config, f.payload, f.state, f.io), /offline/);
  assert.equal(saves, 1);
});

test("temporary PostgreSQL failure retries same event before commit", async () => {
  const f = fixture(); let n = 0;
  f.io.save = async () => { f.order.push("sql"); if (++n === 1) throw Error("offline"); return false; };
  await handleMessage(config, f.payload, f.state, f.io);
  assert.deepEqual(f.order.filter(x => x === "sql" || x.startsWith("commit:")), ["sql", "sql", "commit:9007199254740994"]);
});

test("poison: three attempts, raw data and headers in DLT, then commit", async () => {
  const m = message(true); const f = fixture(m); const logs: string[] = []; f.io.log = s => logs.push(s);
  f.io.send = async request => {
    const sent = request.messages[0]!;
    assert.equal(request.acks, -1); assert.equal(request.topic, "events.dlt");
    assert.equal(sent.value, m.value); assert.equal(sent.key, m.key);
    assert.equal(sent.headers!["source-offset"], source.offset);
    assert.equal(sent.headers!["source-partition"], "1");
    assert.equal(sent.headers!["source-topic"], "events");
    assert.match(String(sent.headers!["error-message"]), /Poison/);
    f.order.push("dlt"); return [];
  };
  await handleMessage(config, f.payload, f.state, f.io);
  assert.equal(logs.filter(s => s.startsWith("processing failed attempt=")).length, 3);
  assert.deepEqual(f.order.filter(x => x === "dlt" || x.startsWith("commit:")), ["dlt", "commit:9007199254740994"]);
});

test("DLT failure retries send without repeating poison attempts", async () => {
  const f = fixture(message(true)); const logs: string[] = []; let sends = 0;
  f.io.log = s => logs.push(s);
  f.io.send = async () => { f.order.push("send"); if (++sends === 1) throw Error("DLT offline"); return []; };
  await handleMessage(config, f.payload, f.state, f.io);
  assert.equal(logs.filter(s => s.startsWith("processing failed attempt=")).length, 3);
  assert.deepEqual(f.order.filter(x => x === "send" || x.startsWith("commit:")), ["send", "send", "commit:9007199254740994"]);
});

test("malformed event retains raw bytes in DLT", async () => {
  const m = { ...message(), value: Buffer.from("{broken") }; let sent = false;
  const io: ProcessingIO = { save: async () => { throw Error("unexpected SQL"); }, send: async r => { assert.equal(r.messages[0]!.value, m.value); sent = true; return []; }, wait: async () => {}, retryAllowed: () => true, log: () => {} };
  await processRecord(config, m, source, io); assert.equal(sent, true);
});

test("sink duplicate uses atomic ON CONFLICT and persistent counter under per-user lock", async () => {
  const queries: string[] = []; let released: boolean | undefined;
  const pool = { connect: async () => ({ query: async (sql: string, values?: unknown[]) => {
    queries.push(sql); if (sql.includes("INSERT INTO processed_events")) { assert.deepEqual(values!.slice(6, 10), [source.consumerId, source.topic, source.partition, source.offset]); return { rowCount: 0 }; }
    return { rowCount: 1, rows: [{ last_processing_order: "0" }] };
  }, release: (destroy: boolean) => { released = destroy; } }) } as unknown as Pick<Pool, "connect">;
  assert.equal(await saveEvent(pool, event, source), true);
  assert.equal(queries[0], "BEGIN"); assert.match(queries[3]!, /ON CONFLICT \(event_id\) DO NOTHING/);
  assert.match(queries[4]!, /duplicates = consumer_stats.duplicates \+ 1/); assert.equal(queries[5], "COMMIT");
  assert.match(queries[2]!, /FOR UPDATE/); assert.equal(released, false);
});

test("sink normal insert and transaction failure", async () => {
  for (const fail of [false, true]) {
    const queries: string[] = []; let destroyed = false;
    const pool = { connect: async () => ({ query: async (sql: string) => {
      queries.push(sql); if (sql.startsWith("INSERT INTO processed_events") && fail) throw Error("SQL error"); return { rowCount: 1, rows: [{ last_processing_order: "0" }] };
    }, release: (bad: boolean) => { destroyed = bad; } }) } as unknown as Pick<Pool, "connect">;
    if (fail) await assert.rejects(saveEvent(pool, event, source), /SQL error/);
    else assert.equal(await saveEvent(pool, event, source), false);
    assert.equal(queries.at(-1), fail ? "ROLLBACK" : "COMMIT"); assert.equal(destroyed, fail);
    assert.ok(!queries.some(q => q.includes("consumer_stats")));
  }
});

for (const signal of ["SIGTERM", "SIGINT"]) test(`lifecycle ${signal} drains active SQL before closing`, async () => {
  const signals = new EventEmitter(); const entered = deferred(); const release = deferred();
  const order: string[] = []; let work: Promise<void> | undefined;
  const events = { GROUP_JOIN: "join", REBALANCING: "rebalance", CRASH: "crash" };
  const resources = {
    consumer: { events, on: () => () => {}, connect: async () => {}, subscribe: async () => {},
      run: async (options: { eachMessage: (p: EachMessagePayload) => Promise<void>; autoCommit: boolean; partitionsConsumedConcurrently: number }) => {
        assert.equal(options.autoCommit, false); assert.equal(options.partitionsConsumedConcurrently, 1);
        work = options.eachMessage(fixture().payload); void work.catch(() => {});
      }, pause: () => {}, commitOffsets: async () => { order.push("commit"); },
      stop: async () => { order.push("stop"); }, disconnect: async () => { order.push("disconnect"); } },
    producer: { connect: async () => {}, send: async () => [], disconnect: async () => { order.push("producer-close"); } },
    pool: { on: () => {}, end: async () => { order.push("pool-close"); }, connect: async () => ({
      query: async (sql: string) => { if (sql.startsWith("INSERT INTO processed_events")) { order.push("sql"); entered.resolve(); await release.promise; } return { rowCount: 1, rows: [{ last_processing_order: "0" }] }; }, release: () => {},
    }) },
  } as unknown as Resources;
  const running = runConsumer(config, resources, signals);
  await entered.promise; signals.emit(signal); release.resolve(); assert.equal(await running, 0); await work;
  assert.deepEqual(order, ["sql", "commit", "stop", "disconnect", "producer-close", "pool-close"]);
  assert.equal(signals.listenerCount(signal), 0);
});

test("duplicate result is committed without another insert", async () => {
  const f = fixture(); f.io.save = async () => { f.order.push("duplicate"); return true; };
  await handleMessage(config, f.payload, f.state, f.io);
  assert.deepEqual(f.order.filter(x => x === "duplicate" || x.startsWith("commit:")), ["duplicate", "commit:9007199254740994"]);
});

test("shutdown deadline waits for in-flight SQL but starts no commit", async () => {
  const signals = new EventEmitter(); const entered = deferred(); const release = deferred();
  const order: string[] = []; let work: Promise<void> | undefined;
  const resources = {
    consumer: {
      events: { GROUP_JOIN: "join", REBALANCING: "rebalance", CRASH: "crash" },
      on: () => () => {}, connect: async () => {}, subscribe: async () => {},
      run: async (options: { eachMessage: (p: EachMessagePayload) => Promise<void> }) => {
        work = options.eachMessage(fixture().payload); void work.catch(() => {});
      },
      pause: () => {}, commitOffsets: async () => { order.push("commit"); },
      stop: async () => { order.push("stop"); }, disconnect: async () => { order.push("disconnect"); },
    },
    producer: { connect: async () => {}, send: async () => [], disconnect: async () => { order.push("producer-close"); } },
    pool: { on: () => {}, end: async () => { order.push("pool-close"); }, connect: async () => ({
      query: async (sql: string) => { if (sql.startsWith("INSERT INTO processed_events")) { order.push("sql"); entered.resolve(); await release.promise; } return { rowCount: 1, rows: [{ last_processing_order: "0" }] }; },
      release: () => {},
    }) },
  } as unknown as Resources;
  const running = runConsumer(config, resources, signals, 5);
  await entered.promise; signals.emit("SIGTERM");
  await new Promise<void>(resolve => setTimeout(resolve, 15));
  assert.deepEqual(order, ["sql"]);
  release.resolve(); await running; await assert.rejects(work!, /deadline/);
  assert.deepEqual(order, ["sql", "stop", "disconnect", "producer-close", "pool-close"]);
});

test("rebalance redelivers saved event; duplicate is committed", async () => {
  const f = fixture(); let inserts = 0;
  f.io.save = async () => { inserts++; return inserts > 1; };
  f.io.commit = async () => { if (inserts === 1) throw Error("REBALANCE_IN_PROGRESS"); f.order.push("commit"); };
  await assert.rejects(handleMessage(config, f.payload, f.state, f.io), /REBALANCE/);
  await handleMessage(config, f.payload, f.state, f.io);
  assert.equal(inserts, 2); assert.deepEqual(f.order.filter(x => x === "commit"), ["commit"]);
});
