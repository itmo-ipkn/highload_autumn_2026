import { createApp } from "./app.ts";
import { config } from "./config.ts";

const app = createApp(config.identity, { logger: config.logger });

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.once(signal, async () => {
    await app.close();
  });
}

try {
  await app.listen({ host: config.host, port: config.port });
  console.log(`Server listening on ${config.host}:${config.port}`);
} catch (error) {
  app.log.error(error);
  process.exitCode = 1;
}
