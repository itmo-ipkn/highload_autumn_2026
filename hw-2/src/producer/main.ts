import { readConfig } from "./config.js";
import { runProducer } from "./producer.js";

try {
  process.exitCode = await runProducer(readConfig());
} catch (error) {
  console.error("Producer startup failed", error);
  process.exitCode = 1;
}
