import { loadEnvFile } from "node:process";
import { existsSync } from "node:fs";

if (existsSync(".env")) loadEnvFile();

const apiUrls = [
  process.env.API_1_URL ?? `http://localhost:${process.env.API_1_PORT}`,
  process.env.API_2_URL ?? `http://localhost:${process.env.API_2_PORT}`,
];

if (apiUrls.some((url) => url.endsWith("undefined"))) {
  throw new Error("Set API_1_URL/API_2_URL or API_1_PORT/API_2_PORT");
}

const createdItems = new Map<string, string[]>();
const maxItemsPerInstance = 1000;

const createUrl = (requestIndex: number, path: string): string => {
  return `${apiUrls[requestIndex % apiUrls.length]}${path}`;
};

const sendRequest = async (requestIndex: number): Promise<void> => {
  const baseUrl = createUrl(requestIndex, "");
  const ids = createdItems.get(baseUrl) ?? [];

  const choice = Math.random();
  let method = choice < 0.4 ? "POST" : choice < 0.8 ? "GET" : "PUT";

  if (method !== "POST" && ids.length === 0) {
    method = "POST";
  }

  const intentionalError = Math.random() < 0.05;

  const id =
    method === "POST" ? undefined : ids[Math.floor(Math.random() * ids.length)];

  const missing = method === "GET" && intentionalError;

  const path =
    method === "POST"
      ? "/items"
      : `/items/${encodeURIComponent(
          missing ? `load-missing-${requestIndex}` : id!,
        )}`;

  const url = createUrl(requestIndex, path);

  try {
    const response = await fetch(url, {
      method,
      ...(method === "GET"
        ? {}
        : {
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              name: `Item ${requestIndex}`,
              price:
                method === "POST" && intentionalError
                  ? -1
                  : Math.floor(Math.random() * 1000) + 1,
            }),
          }),
    });

    if (!response.ok) {
      if (response.status === 404 && method !== "POST" && !missing) {
        const current = createdItems.get(baseUrl) ?? [];
        createdItems.set(
          baseUrl,
          current.filter((value) => value !== id),
        );
      }

      await response.body?.cancel();
      console.error(`${method} ${url}: HTTP ${response.status}`);
      return;
    }

    if (method === "POST") {
      const item: unknown = await response.json();

      if (
        !item ||
        typeof item !== "object" ||
        !("id" in item) ||
        typeof item.id !== "string" ||
        !item.id.trim()
      ) {
        throw new Error("POST response has no valid product id");
      }

      const current = createdItems.get(baseUrl) ?? [];
      current.push(item.id);
      if (current.length > maxItemsPerInstance) {
        current.shift();
      }
      createdItems.set(baseUrl, current);
    } else {
      await response.body?.cancel();
    }
  } catch (error) {
    console.error(`${method} ${url} failed:`, error);
  }
};

const stages = [
  { durationSeconds: 30, rps: 5 },
  { durationSeconds: 30, rps: 10 },
  { durationSeconds: 30, rps: 20 },
  { durationSeconds: Infinity, rps: 30 },
];

let sent = 0;
let elapsedTicks = 0;
let requestCredit = 0;

const tickMilliseconds = 100;

const timer = setInterval(() => {
  elapsedTicks++;

  const elapsedSeconds = (elapsedTicks * tickMilliseconds) / 1000;
  let remaining = elapsedSeconds;

  const stage = stages.find(({ durationSeconds }) => {
    if (remaining < durationSeconds) {
      return true;
    }

    remaining -= durationSeconds;
    return false;
  })!;

  requestCredit += (stage.rps * tickMilliseconds) / 1000;

  const toSend = Math.floor(requestCredit);
  requestCredit -= toSend;

  for (let i = 0; i < toSend; i++) {
    void sendRequest(sent);
    sent++;
  }

  if (toSend > 0) {
    console.log(`Target: ${stage.rps} RPS; scheduled: ${sent}`);
  }
}, tickMilliseconds);

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => clearInterval(timer));
}
