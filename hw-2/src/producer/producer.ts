import { Kafka, Partitioners, type Producer } from "kafkajs";
import { setTimeout as delay } from "node:timers/promises";
import type { Config } from "./config.js";
import { createGenerator, toMessage, usersFor } from "./events.js";
import type { UserEvent } from "../shared/event.js";

export const retry = { retries: 3, initialRetryTime: 300, maxRetryTime: 1000 };
export function makeProducer(config: Config) {
  return new Kafka({
    clientId: config.producerId,
    brokers: config.brokers,
    connectionTimeout: 3000,
    requestTimeout: 10000,
    enforceRequestTimeout: true,
    // No nested client retries; the producer owns the finite retry budget.
    retry: { retries: 0 },
  }).producer({
    idempotent: true,
    maxInFlightRequests: 1,
    allowAutoTopicCreation: false,
    createPartitioner: Partitioners.DefaultPartitioner,
    retry,
  });
}

async function sendEvent(
  producer: Pick<Producer, "send">,
  config: Config,
  event: UserEvent,
) {
  try {
    const metadata = await producer.send({
      topic: config.topic,
      acks: -1,
      timeout: 5000,
      messages: [toMessage(event, config.keyMode)],
    });
    console.log(
      `${config.producerId} | sent event_id=${event.event_id} user=${event.user_id} seq=${event.seq} type=${event.type} poison=${event.payload.poison === true} partition=${metadata[0]?.partition}`,
    );
  } catch (error) {
    console.error(
      `${config.producerId} | send failed event_id=${event.event_id} user=${event.user_id} seq=${event.seq}`,
      error,
    );
    throw error;
  }
}

async function waitForNextEvent(intervalMs: number, signal: AbortSignal) {
  try {
    await delay(intervalMs, undefined, { signal });
  } catch (error) {
    if (!signal.aborted) {
      throw error;
    }
  }
}

export async function runProducer(
  config: Config,
  producer: Pick<Producer, "connect" | "send" | "disconnect"> = makeProducer(
    config,
  ),
  signals: NodeJS.EventEmitter = process,
) {
  const stop = new AbortController();

  const log = (message: string) =>
    console.log(`${config.producerId} | ${message}`);

  const shutdown = (signal: string) => {
    if (!stop.signal.aborted) {
      log(`stopping signal=${signal}`);
      stop.abort();
    }
  };

  const term = () => shutdown("SIGTERM");
  const interrupt = () => shutdown("SIGINT");

  signals.on("SIGTERM", term);
  signals.on("SIGINT", interrupt);

  let failed = false;

  try {
    await producer.connect();
    log("connected");

    const next = createGenerator(
      usersFor(config.producerIndex, config.userCount),
      config.poisonRate,
    );

    while (!stop.signal.aborted) {
      const event = next();
      if (event) {
        await sendEvent(producer, config, event);
      }
      if (!stop.signal.aborted) {
        await waitForNextEvent(config.intervalMs, stop.signal);
      }
    }
  } catch (error) {
    failed = true;
    console.error(`${config.producerId} | producer failed`, error);
  } finally {
    try {
      await producer.disconnect();
    } catch (error) {
      failed = true;
      console.error(`${config.producerId} | disconnect failed`, error);
    }

    signals.removeListener("SIGTERM", term);
    signals.removeListener("SIGINT", interrupt);

    log(`shutdown complete exitCode=${failed ? 1 : 0}`);
  }
  return failed ? 1 : 0;
}
