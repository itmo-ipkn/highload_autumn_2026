import { readConsumerConfig } from "./config.js";
import { runConsumer } from "./consumer.js";

try {
  process.exitCode = await runConsumer(readConsumerConfig());
} catch (error) {
  console.error("Consumer startup failed", error);
  process.exitCode = 1;
}
