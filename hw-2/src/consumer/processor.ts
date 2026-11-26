import type { KafkaMessage, Producer } from "kafkajs";
import type { UserEvent } from "../shared/event.js";
import type { ConsumerConfig } from "./config.js";
import { assertUserEvent } from "./event-validation.js";
import type { Source } from "./sink.js";

export interface ProcessingIO {
  save: (event: UserEvent, source: Source) => Promise<boolean>;
  send: Pick<Producer, "send">["send"];
  wait: (ms: number) => Promise<void>;
  retryAllowed: () => boolean;
  log: (message: string) => void;
}
export function parseEvent(value: Buffer | null): UserEvent {
  const event: unknown = JSON.parse(value?.toString("utf8") ?? "null");
  assertUserEvent(event);
  return event;
}

export async function processRecord(
  config: ConsumerConfig,
  message: KafkaMessage,
  source: Source,
  io: ProcessingIO,
) {
  let event: UserEvent | undefined;
  let failure: unknown;
  for (let attempt = 1; attempt <= config.maxAttempts; attempt++) {
    await io.wait(config.processingDelayMs);
    try {
      event = parseEvent(message.value);
      if (event.payload.poison === true) {
        throw Error("Poison event");
      }
      failure = undefined;
      break;
    } catch (error) {
      failure = error;
      io.log(
        `processing failed attempt=${attempt} offset=${source.offset} error=${String(error)}`,
      );
      if (attempt < config.maxAttempts) {
        if (!io.retryAllowed()) {
          throw Error("Stopping before processing retry");
        }
        await io.wait(config.retryDelayMs);
      }
    }
  }
  // Infrastructure retries never consume the poison attempt budget or route SQL errors to DLT.
  for (;;) {
    try {
      if (failure !== undefined) {
        await io.send({
          topic: config.dltTopic,
          acks: -1,
          timeout: 5000,
          messages: [
            {
              key: message.key,
              value: message.value,
              headers: {
                ...message.headers,
                "error-message": String(failure),
                "processing-attempts": String(config.maxAttempts),
                "source-topic": source.topic,
                "source-partition": String(source.partition),
                "source-offset": source.offset,
              },
            },
          ],
        });
        io.log(`dlt sent offset=${source.offset}`);
      } else {
        const duplicate = await io.save(event!, source);

        io.log(
          `${duplicate ? "duplicate" : "saved"} event_id=${event!.event_id} offset=${source.offset}`,
        );
      }
      return;
    } catch (error) {
      io.log(
        `infrastructure error offset=${source.offset} error=${String(error)}`,
      );
      if (!io.retryAllowed()) throw error;
      await io.wait(config.retryDelayMs);
    }
  }
}
