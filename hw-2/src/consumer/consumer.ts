import {
  Kafka,
  type Consumer,
  type EachMessagePayload,
  type Producer,
} from "kafkajs";
import { setTimeout as delay } from "node:timers/promises";
import type { Pool } from "pg";
import type { ConsumerConfig } from "./config.js";
import { processRecord } from "./processor.js";
import { saveEvent, type Source } from "./sink.js";
import { makePool } from "../shared/postgres.js";
import type { UserEvent } from "../shared/event.js";

interface ConsumerState {
  stopping: boolean;
  deadlineAt: number;
}
export interface MessageIO {
  save: (event: UserEvent, source: Source) => Promise<boolean>;
  send: Pick<Producer, "send">["send"];
  commit: Consumer["commitOffsets"];
  log: (message: string) => void;
}

export async function handleMessage(
  config: ConsumerConfig,
  payload: EachMessagePayload,
  state: ConsumerState,
  io: MessageIO,
  heartbeatEveryMs = 1000,
) {
  const { topic, partition, message, heartbeat } = payload;

  if (state.stopping || Date.now() >= state.deadlineAt) {
    return;
  }

  let heartbeatFailure: unknown;

  function check() {
    if (heartbeatFailure) {
      throw heartbeatFailure;
    }

    if (Date.now() >= state.deadlineAt)
      throw Error("Shutdown deadline expired");
  }

  await heartbeat();
  const timerStop = new AbortController();

  const heartbeats = (async () => {
    while (!timerStop.signal.aborted) {
      try {
        await delay(heartbeatEveryMs, undefined, { signal: timerStop.signal });
      } catch {
        break;
      }
      if (timerStop.signal.aborted) break;
      try {
        await heartbeat();
      } catch (error) {
        heartbeatFailure = error;
        break;
      }
    }
  })();

  try {
    io.log(
      `processing topic=${topic} partition=${partition} offset=${message.offset}`,
    );
    await processRecord(
      config,
      message,
      {
        consumerId: config.consumerId,
        topic,
        partition,
        offset: message.offset,
      },
      {
        save: (event, source) => {
          check();
          return io.save(event, source);
        },
        send: (request) => {
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
          if (state.stopping)
            throw Error("Stopping before retry or processing");
        },
        retryAllowed: () =>
          !state.stopping && Date.now() < state.deadlineAt && !heartbeatFailure,
        log: io.log,
      },
    );
  } finally {
    timerStop.abort();
    await heartbeats;
  }

  check();
  await heartbeat();
  check();
  await io.commit([
    { topic, partition, offset: (BigInt(message.offset) + 1n).toString() },
  ]);
  io.log(
    `committed topic=${topic} partition=${partition} offset=${message.offset}`,
  );
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
  | "commitOffsets"
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

  const state: ConsumerState = { stopping: false, deadlineAt: Infinity };

  const log = (message: string) =>
    console.log(`${config.consumerId} | ${message}`);

  let active: Promise<void> | undefined;

  let assignment: Array<{ topic: string; partitions: number[] }> = [];

  let finish!: () => void;

  const finished = new Promise<void>((resolve) => {
    finish = resolve;
  });

  let failed = false;

  function shutdown(signal: string) {
    if (state.stopping) {
      return;
    }
    state.stopping = true;
    state.deadlineAt = Date.now() + shutdownMs;

    log(`stopping signal=${signal}`);
    try {
      if (assignment.length) {
        consumer.pause(assignment);
      }
    } catch (error) {
      log(`pause failed ${String(error)}`);
    }
    finish();
  }
  const term = () => shutdown("SIGTERM");
  const interrupt = () => shutdown("SIGINT");

  signals.on("SIGTERM", term);
  signals.on("SIGINT", interrupt);

  const removeJoin = consumer.on(consumer.events.GROUP_JOIN, (event) => {
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
        partitionsConsumedConcurrently: 1,
        eachMessage: (payload) => {
          const work = handleMessage(config, payload, state, {
            save: (event, source) => saveEvent(pool, event, source),
            send: (request) => producer.send(request),
            commit: (offsets) => consumer.commitOffsets(offsets),
            log,
          });
          active = work;
          return work.finally(() => {
            if (active === work) {
              active = undefined;
            }
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
      log(`message ended during shutdown ${String(error)}`);
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
    removeJoin();
    removeRebalance();
    removeCrash();

    signals.removeListener("SIGTERM", term);
    signals.removeListener("SIGINT", interrupt);

    log(`shutdown complete exitCode=${failed ? 1 : 0}`);
  }
  return failed ? 1 : 0;
}
