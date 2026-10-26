import { loadEnvFile } from "node:process";
import { readIdentity, type InstanceIdentity } from "./monitoring.ts";

export interface AppConfig {
  host: string;
  port: number;
  logger: boolean;
  identity: InstanceIdentity;
}

export function readConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  const port = Number(env.PORT ?? 3000);
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("PORT must be an integer between 1 and 65535");
  }

  return {
    host: "0.0.0.0",
    port,
    logger: true,
    identity: readIdentity(env),
  };
}

loadEnvFile();

export const config = readConfig(process.env);
