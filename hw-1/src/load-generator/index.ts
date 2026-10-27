import { loadEnvFile } from "node:process";
import { existsSync } from "node:fs";

if (existsSync(".env")) loadEnvFile();

const apiUrls = [
  `http://localhost:${process.env.API_1_PORT}`,
  `http://localhost:${process.env.API_2_PORT}`,
];

if (apiUrls.some((url) => url.endsWith("undefined"))) {
  throw new Error("API_1_PORT and API_2_PORT must be set in .env");
}

const createUrl = (requestIndex: number, path: string): string => {
  const baseUrl = apiUrls[requestIndex % apiUrls.length];

  return `${baseUrl}${path}`;
};

const createItem = async (requestIndex: number): Promise<void> => {
  const url = createUrl(requestIndex, "/items");

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        name: `Item ${requestIndex}`,
        price: Math.floor(Math.random() * 1000) + 1,
      }),
    });

    if (!response.ok) {
      console.error(`POST ${url}: HTTP ${response.status}`);
    }
  } catch (error) {
    console.error(`POST ${url} failed:`, error);
  }
};

const stages = [
  { durationSeconds: 30, rps: 5 },
  { durationSeconds: 30, rps: 10 },
  { durationSeconds: 30, rps: 20 },
  { durationSeconds: Infinity, rps: 30 },
];

const startedAt = Date.now();
let sent = 0;

const timer = setInterval(() => {
  const elapsedSeconds = (Date.now() - startedAt) / 1000;

  let remaining = elapsedSeconds;

  const stage = stages.find(({ durationSeconds }) => {
    if (remaining < durationSeconds) {
      return true;
    }

    remaining -= durationSeconds;
    return false;
  })!;

  const expected = Math.floor(
    elapsedSeconds < 30
      ? elapsedSeconds * 5
      : elapsedSeconds < 60
        ? 30 * 5 + (elapsedSeconds - 30) * 10
        : elapsedSeconds < 90
          ? 30 * 5 + 30 * 10 + (elapsedSeconds - 60) * 20
          : 30 * 5 + 30 * 10 + 30 * 20 + (elapsedSeconds - 90) * 30,
  );

  const toSend = expected - sent;

  for (let i = 0; i < toSend; i++) {
    void createItem(sent);
    sent++;
  }

  if (toSend > 0) {
    console.log(`Target: ${stage.rps} RPS; scheduled: ${sent}`);
  }
}, 100);

process.on("SIGINT", () => clearInterval(timer));
