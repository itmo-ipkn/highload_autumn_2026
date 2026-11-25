import { runVerify } from "./verify.js";
const controller = new AbortController();
const stop = () => controller.abort(Error("Verification interrupted"));

process.on("SIGINT", stop);
process.on("SIGTERM", stop);

try {
  await runVerify(process.env, controller.signal);
} catch (error) {
  console.error(
    `verify: ${error instanceof Error ? error.message : String(error)}`,
  );
  process.exitCode = 1;
} finally {
  process.off("SIGINT", stop);
  process.off("SIGTERM", stop);
}
