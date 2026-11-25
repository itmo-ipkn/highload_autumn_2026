import test from "node:test";
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import type { EachBatchPayload, KafkaMessage } from "kafkajs";
import { readConsumerConfig } from "../src/consumer-config.js";
import { handleBatch, runConsumer, type BatchIO, type BatchState, type Resources } from "../src/consumer.js";
import { processRecord, type ProcessingIO } from "../src/processor.js";
import { saveEvent } from "../src/sink.js";
import { createGenerator } from "../src/events.js";
import type { Pool } from "pg";
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
function fixture(messages = [message(), { ...message(), offset: "9007199254740994" }]) {
  const order: string[] = [];
  const state: BatchState = { stopping: false, expired: false };
  const payload: EachBatchPayload = {
    batch: { topic: "events", partition: 1, highWatermark: "0", messages,
      isEmpty: () => false, firstOffset: () => messages[0]!.offset, lastOffset: () => messages.at(-1)!.offset,
      offsetLag: () => "0", offsetLagLow: () => "0" },
    heartbeat: async () => {}, isRunning: () => true, isStale: () => false,
    resolveOffset: offset => { order.push(`resolve:${offset}`); },
    commitOffsetsIfNecessary: async offsets => { order.push(`commit:${offsets!.topics[0]!.partitions[0]!.offset}`); },
    uncommittedOffsets: () => ({ topics: [] }), pause: () => () => {},
  };
  const io: BatchIO = { save: async () => { order.push("save"); return false; }, send: async () => { order.push("dlt"); return []; }, log: () => {} };
  return { payload, io, state, order };
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

test("each message commits next offset with exact bigint precision, then resolves", async () => {
  const f = fixture(); await handleBatch(config, f.payload, f.state, f.io);
  assert.deepEqual(f.order, ["save", "commit:9007199254740994", "resolve:9007199254740993", "save", "commit:9007199254740995", "resolve:9007199254740994"]);
});

for (const name of ["REBALANCE_IN_PROGRESS", "ILLEGAL_GENERATION", "UNKNOWN_MEMBER_ID", "request timeout"]) {
  test(`commit ${name}: no next message, no resolve, no retry`, async () => {
    const f = fixture(); f.payload.commitOffsetsIfNecessary = async () => { f.order.push("commit"); throw Error(name); };
    await assert.rejects(handleBatch(config, f.payload, f.state, f.io), new RegExp(name));
    assert.deepEqual(f.order, ["save", "commit"]);
  });
}

for (const when of ["before", "after"]) test(`heartbeat failure ${when} SQL stops batch without commit`, async () => {
  const f = fixture(); let n = 0;
  f.payload.heartbeat = async () => { if (++n === (when === "before" ? 1 : 2)) throw Error("ILLEGAL_GENERATION"); };
  await assert.rejects(handleBatch(config, f.payload, f.state, f.io), /ILLEGAL_GENERATION/);
  assert.deepEqual(f.order, when === "before" ? [] : ["save"]);
});

test("heartbeat failure during in-flight SQL waits for SQL and never commits", async () => {
  const f = fixture(); const entered = deferred(); const release = deferred(); const lost = deferred(); let n = 0;
  f.io.save = async () => { f.order.push("save"); entered.resolve(); await release.promise; return false; };
  f.payload.heartbeat = async () => { if (++n > 1) { await entered.promise; lost.resolve(); throw Error("UNKNOWN_MEMBER_ID"); } };
  const work = handleBatch(config, f.payload, f.state, f.io, 1);
  const rejected = assert.rejects(work, /UNKNOWN_MEMBER_ID/);
  await entered.promise; await lost.promise; assert.deepEqual(f.order, ["save"]);
  release.resolve(); await rejected; assert.deepEqual(f.order, ["save"]);
});

test("stale batch after SQL is not committed", async () => {
  const f = fixture(); let stale = false;
  f.payload.isStale = () => stale; f.io.save = async () => { stale = true; return false; };
  await assert.rejects(handleBatch(config, f.payload, f.state, f.io), /no longer usable/);
  assert.deepEqual(f.order, []);
});

test("signal during processing drains current save and commit, never starts next message", async () => {
  const f = fixture(); const entered = deferred(); const release = deferred();
  f.io.save = async () => { f.order.push("save"); entered.resolve(); await release.promise; return false; };
  const work = handleBatch(config, f.payload, f.state, f.io);
  await entered.promise; f.state.stopping = true; release.resolve(); await work;
  assert.deepEqual(f.order, ["save", "commit:9007199254740994", "resolve:9007199254740993"]);
});

test("expired shutdown deadline prevents commit of completed SQL", async () => {
  const f = fixture(); f.io.save = async () => { f.state.expired = true; return false; };
  await assert.rejects(handleBatch(config, f.payload, f.state, f.io), /no longer usable/);
  assert.deepEqual(f.order, []);
});

for (const kind of ["sql", "dlt"]) test(`${kind} failure has no commit and retries infrastructure without skipping`, async () => {
  const f = fixture([message(kind === "dlt")]); let attempts = 0;
  const fail = async () => { attempts++; f.state.stopping = true; throw Error("offline"); };
  if (kind === "sql") f.io.save = fail; else f.io.send = fail;
  await assert.rejects(handleBatch(config, f.payload, f.state, f.io), /offline/);
  assert.equal(attempts, 1); assert.deepEqual(f.order, []);
});

test("temporary PostgreSQL failure retries the same event and commits only after recovery", async () => {
  const f = fixture([message()]); let n = 0;
  f.io.save = async () => { f.order.push("sql"); if (++n === 1) throw Error("offline"); return false; };
  await handleBatch(config, f.payload, f.state, f.io);
  assert.deepEqual(f.order, ["sql", "sql", "commit:9007199254740994", "resolve:9007199254740993"]);
});

test("poison: exactly three attempts, raw key/value and source/error headers in DLT, then commit", async () => {
  const m = message(true); const f = fixture([m]); const logs: string[] = []; f.io.log = s => logs.push(s);
  f.io.send = async request => {
    assert.equal(request.acks, -1); assert.equal(request.topic, "events.dlt");
    const sent = request.messages[0]!; assert.equal(sent.value, m.value); assert.equal(sent.key, m.key);
    assert.equal(sent.headers!["source-offset"], source.offset); assert.equal(sent.headers!["source-partition"], "1");
    assert.equal(sent.headers!["source-topic"], "events"); assert.match(String(sent.headers!["error-message"]), /Poison/);
    f.order.push("dlt"); return [];
  };
  await handleBatch(config, f.payload, f.state, f.io);
  assert.equal(logs.filter(s => s.startsWith("processing failed attempt=")).length, 3);
  assert.deepEqual(f.order, ["dlt", "commit:9007199254740994", "resolve:9007199254740993"]);
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

for (const signal of ["SIGTERM", "SIGINT"]) test(`lifecycle ${signal} during batch drains before stop and disconnect`, async () => {
  const signals = new EventEmitter(); const entered = deferred(); const release = deferred();
  const f = fixture(); const order: string[] = []; let work: Promise<void> | undefined;
  const events = { GROUP_JOIN: "join", REBALANCING: "rebalance", CRASH: "crash" };
  const resources = {
    consumer: { events, on: () => () => {}, connect: async () => {}, subscribe: async () => {},
      run: async (options: { eachBatch: (p: EachBatchPayload) => Promise<void>; autoCommit: boolean; eachBatchAutoResolve: boolean }) => {
        assert.equal(options.autoCommit, false); assert.equal(options.eachBatchAutoResolve, false);
        work = options.eachBatch(f.payload); void work.catch(() => {});
      }, pause: () => {}, stop: async () => { order.push("stop"); }, disconnect: async () => { order.push("disconnect"); } },
    producer: { connect: async () => {}, send: async () => [], disconnect: async () => { order.push("producer-close"); } },
    pool: { on: () => {}, end: async () => { order.push("pool-close"); }, connect: async () => ({
      query: async (sql: string) => { if (sql.startsWith("INSERT INTO processed_events")) { order.push("sql"); entered.resolve(); await release.promise; } return { rowCount: 1, rows: [{ last_processing_order: "0" }] }; }, release: () => {},
    }) },
  } as unknown as Resources;
  const running = runConsumer(config, resources, signals);
  await entered.promise; signals.emit(signal); signals.emit(signal); assert.deepEqual(order, ["sql"]);
  release.resolve(); assert.equal(await running, 0); await work;
  assert.deepEqual(f.order, ["commit:9007199254740994", "resolve:9007199254740993"]);
  assert.deepEqual(order, ["sql", "stop", "disconnect", "producer-close", "pool-close"]);
  assert.equal(signals.listenerCount(signal), 0);
});

test("duplicate result is committed without creating another event", async () => {
  const f = fixture([message()]); f.io.save = async () => { f.order.push("duplicate"); return true; };
  await handleBatch(config, f.payload, f.state, f.io);
  assert.deepEqual(f.order, ["duplicate", "commit:9007199254740994", "resolve:9007199254740993"]);
});

test("DLT transient failure retries send without repeating poison attempts", async () => {
  const f = fixture([message(true)]); const logs: string[] = []; let sends = 0;
  f.io.log = s => logs.push(s);
  f.io.send = async () => { f.order.push("send"); if (++sends === 1) throw Error("DLT offline"); return []; };
  await handleBatch(config, f.payload, f.state, f.io);
  assert.equal(logs.filter(s => s.startsWith("processing failed attempt=")).length, 3);
  assert.deepEqual(f.order, ["send", "send", "commit:9007199254740994", "resolve:9007199254740993"]);
});

test("runtime blocks queued batch after commit error; new GROUP_JOIN permits fresh delivery", async () => {
  const signals = new EventEmitter(); const listeners = new Map<string, (event: any) => void>();
  let inserts = 0; let commitCalls = 0;
  const f = fixture([message()]);
  f.payload.commitOffsetsIfNecessary = async () => { if (++commitCalls === 1) throw Error("ILLEGAL_GENERATION"); };
  const resources = {
    consumer: { events: { GROUP_JOIN: "join", REBALANCING: "rebalance", CRASH: "crash" },
      on: (name: string, listener: (event: any) => void) => { listeners.set(name, listener); return () => listeners.delete(name); },
      connect: async () => {}, subscribe: async () => {},
      run: async (options: { eachBatch: (p: EachBatchPayload) => Promise<void> }) => {
        listeners.get("join")!({ payload: { memberAssignment: { events: [1] } } });
        await assert.rejects(options.eachBatch(f.payload), /ILLEGAL_GENERATION/);
        await assert.rejects(options.eachBatch(f.payload), /ILLEGAL_GENERATION/);
        assert.equal(inserts, 1); assert.equal(commitCalls, 1);
        listeners.get("join")!({ payload: { memberAssignment: { events: [1] } } });
        await options.eachBatch(f.payload);
        assert.equal(inserts, 2); assert.equal(commitCalls, 2);
        signals.emit("SIGTERM");
      }, pause: () => {}, stop: async () => {}, disconnect: async () => {} },
    producer: { connect: async () => {}, send: async () => [], disconnect: async () => {} },
    pool: { on: () => {}, end: async () => {}, connect: async () => ({
      query: async (sql: string) => { if (sql.startsWith("INSERT INTO processed_events")) inserts++; return { rowCount: 1, rows: [{ last_processing_order: "0" }] }; }, release: () => {},
    }) },
  } as unknown as Resources;
  assert.equal(await runConsumer(config, resources, signals), 0);
});
