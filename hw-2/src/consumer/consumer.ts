import {
  Kafka,
  type Consumer,
  type EachBatchPayload,
  type Producer,
} from "kafkajs";
import { setTimeout as delay } from "node:timers/promises";
import type { Pool } from "pg";
import type { ConsumerConfig } from "./config.js";
import { processRecord } from "./processor.js";
import { saveEvent, type Source } from "./sink.js";
import { makePool } from "../shared/postgres.js";
import type { UserEvent } from "../shared/event.js";

export interface BatchState {
  stopping: boolean;
  expired: boolean;
}
export interface BatchIO {
  save: (event: UserEvent, source: Source) => Promise<boolean>;
  send: Pick<Producer, "send">["send"];
  log: (message: string) => void;
}

export async function handleBatch(
  config: ConsumerConfig,
  payload: EachBatchPayload,
  state: BatchState,
  io: BatchIO,
  heartbeatEveryMs = 1000,
) {
  const {
    batch,
    heartbeat,
    isRunning,
    isStale,
    commitOffsetsIfNecessary,
    resolveOffset,
  } = payload;
  let heartbeatFailure: unknown;
  function check() {
    if (heartbeatFailure) throw heartbeatFailure;
    if (state.expired || !isRunning() || isStale())
      throw Error("Batch no longer usable");
  }
  async function pulse() {
    check();
    await heartbeat();
    check();
  }
  for (const message of batch.messages) {
    if (state.stopping) {
      break;
    }
    check();
    await pulse();
    const timerStop = new AbortController();
    // One sequential heartbeat loop, with no detached SQL or send promises.
    const heartbeats = (async () => {
      while (!timerStop.signal.aborted) {
        try {
          await delay(heartbeatEveryMs, undefined, {
            signal: timerStop.signal,
          });
        } catch {
          break;
        }
        if (timerStop.signal.aborted) {
          break;
        }
        try {
          await pulse();
        } catch (error) {
          heartbeatFailure = error;
          break;
        }
      }
    })();
    try {
      io.log(
        `processing topic=${batch.topic} partition=${batch.partition} offset=${message.offset}`,
      );
      await processRecord(
        config,
        message,
        {
          consumerId: config.consumerId,
          topic: batch.topic,
          partition: batch.partition,
          offset: message.offset,
        },
        {
          save: async (event, source) => {
            check();
            return io.save(event, source);
          },
          send: async (request) => {
            check();
            return io.send(request);
          },
          wait: async (ms) => {
            const end = Date.now() + ms;
            do {
              check();
              await delay(Math.min(1000, Math.max(0, end - Date.now())));
            } while (Date.now() < end);
            check();
          },
          retryAllowed: () =>
            !state.stopping &&
            !state.expired &&
            !heartbeatFailure &&
            isRunning() &&
            !isStale(),
          log: io.log,
        },
      );
    } finally {
      timerStop.abort();
      await heartbeats;
    }
    check();
    await pulse();
    // Explicit offsets bypass KafkaJS runner's public commitOffsets retry loop.
    // Do not retry this commit or catch-and-continue on any failure.
    await commitOffsetsIfNecessary({
      topics: [
        {
          topic: batch.topic,
          partitions: [
            {
              partition: batch.partition,
              offset: (BigInt(message.offset) + 1n).toString(),
            },
          ],
        },
      ],
    });
    resolveOffset(message.offset); // Local progress only, never the durable acknowledgement.
    io.log(
      `committed topic=${batch.topic} partition=${batch.partition} offset=${message.offset}`,
    );
  }
}

export function makeConsumerResources(config: ConsumerConfig) {
  const kafka = new Kafka({
    clientId: config.consumerId,
    brokers: config.brokers,
    connectionTimeout: 3000,
    requestTimeout: 10000,
    enforceRequestTimeout: true,
    retry: { retries: 3, initialRetryTime: 300, maxRetryTime: 1000 },
  });
  return {
    consumer: kafka.consumer({
      groupId: config.groupId,
      allowAutoTopicCreation: false,
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
    }),
    producer: kafka.producer({
      idempotent: true,
      maxInFlightRequests: 1,
      allowAutoTopicCreation: false,
      retry: { retries: 3, initialRetryTime: 300, maxRetryTime: 1000 },
    }),
    pool: makePool(config.databaseUrl),
  };
}

type ConsumerPort = Pick<
  Consumer,
  | "connect"
  | "subscribe"
  | "run"
  | "pause"
  | "stop"
  | "disconnect"
  | "on"
  | "events"
>;

export interface Resources {
  consumer: ConsumerPort;
  producer: Pick<Producer, "connect" | "send" | "disconnect">;
  pool: Pick<Pool, "connect" | "end" | "on">;
}

export async function runConsumer(
  config: ConsumerConfig,
  resources: Resources = makeConsumerResources(config),
  signals: NodeJS.EventEmitter = process,
  shutdownMs = 30000,
) {
  const { consumer, producer, pool } = resources;

  const state: BatchState = { stopping: false, expired: false };

  const log = (message: string) =>
    console.log(`${config.consumerId} | ${message}`);

  let active: Promise<void> | undefined;

  let batchFailure: unknown;

  let assignment: Array<{ topic: string; partitions: number[] }> = [];

  let finish!: () => void;

  const finished = new Promise<void>((resolve) => {
    finish = resolve;
  });

  let failed = false;

  let deadline: ReturnType<typeof setTimeout> | undefined;

  function shutdown(signal: string) {
    if (state.stopping) {
      return;
    }
    state.stopping = true;

    log(`stopping signal=${signal}`);
    try {
      if (assignment.length) {
        consumer.pause(assignment);
      }
    } catch (error) {
      log(`pause failed ${String(error)}`);
    }
    deadline = setTimeout(() => {
      state.expired = true;
      log("shutdown deadline: no further commits");
    }, shutdownMs);
    finish();
  }
  const term = () => shutdown("SIGTERM");
  const interrupt = () => shutdown("SIGINT");

  signals.on("SIGTERM", term);
  signals.on("SIGINT", interrupt);

  const removeJoin = consumer.on(consumer.events.GROUP_JOIN, (event) => {
    batchFailure = undefined; // GROUP_JOIN is emitted after join+sync, never a revoke barrier.
    assignment = Object.entries(event.payload.memberAssignment).map(
      ([topic, partitions]) => ({ topic, partitions }),
    );
    log(`group joined assignment=${JSON.stringify(assignment)}`);

    if (state.stopping && assignment.length) {
      consumer.pause(assignment);
    }
  });

  const removeRebalance = consumer.on(consumer.events.REBALANCING, () =>
    log("rebalancing"),
  );

  const removeCrash = consumer.on(consumer.events.CRASH, (event) => {
    log(`consumer crashed ${String(event.payload.error)}`);
    // Let KafkaJS handle recoverable rejoin/restart. Fatal failures close all resources.
    if (!event.payload.restart) {
      failed = !state.stopping;
      shutdown("CRASH");
    }
  });

  const poolError = (error: Error) =>
    log(`idle PostgreSQL error ${String(error)}`);

  pool.on("error", poolError);

  try {
    await consumer.connect();
    await producer.connect();

    log("connected");

    await consumer.subscribe({ topics: [config.topic], fromBeginning: true });

    if (!state.stopping)
      await consumer.run({
        autoCommit: false,
        eachBatchAutoResolve: false,
        partitionsConsumedConcurrently: 1,
        eachBatch: (payload) => {
          if (state.stopping) {
            return Promise.resolve();
          }

          if (batchFailure) {
            return Promise.reject(batchFailure);
          }

          const work = handleBatch(config, payload, state, {
            save: (event, source) => saveEvent(pool, event, source),
            send: (request) => producer.send(request),
            log,
          });

          active = work;

          return work
            .catch((error) => {
              // KafkaJS drains queued batches before rejoining. Do not let those callbacks
              // start new side effects after a detected failure in the old group session.
              batchFailure = error;
              throw error;
            })
            .finally(() => {
              if (active === work) active = undefined;
            });
        },
      });
    await finished;
  } catch (error) {
    failed = true;
    log(`consumer failed ${String(error)}`);
  } finally {
    state.stopping = true;

    try {
      await active;
    } catch (error) {
      log(`batch ended during shutdown ${String(error)}`);
    }

    for (const close of [
      () => consumer.stop(),
      () => consumer.disconnect(),
      () => producer.disconnect(),
      () => pool.end(),
    ]) {
      try {
        await close();
      } catch (error) {
        failed = true;
        log(`cleanup failed ${String(error)}`);
      }
    }
    if (deadline) {
      clearTimeout(deadline);
    }
    removeJoin();
    removeRebalance();
    removeCrash();

    signals.removeListener("SIGTERM", term);
    signals.removeListener("SIGINT", interrupt);

    log(`shutdown complete exitCode=${failed ? 1 : 0}`);
  }
  return failed ? 1 : 0;
}
