function numberFromEnvironment(name, fallback) {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`${name} must be a non-negative number`);
  }
  return value;
}

const appUrls = (process.env.APP_URLS ?? "").split(",").map((url) => url.trim()).filter(Boolean);
if (appUrls.length !== 2 || appUrls.some((url) => !/^https?:\/\//.test(url))) {
  throw new Error("APP_URLS must contain two comma-separated HTTP URLs");
}

const targetRps = numberFromEnvironment("LOAD_TARGET_RPS", 30);
const rampSeconds = numberFromEnvironment("LOAD_RAMP_SECONDS", 120);
const notFoundRate = numberFromEnvironment("LOAD_404_RATE", 0.03);
const serverErrorRate = numberFromEnvironment("LOAD_500_RATE", 0.02);
if (targetRps <= 0 || rampSeconds <= 0 || notFoundRate + serverErrorRate >= 1) {
  throw new Error("Load settings require positive RPS and ramp time, and error rates totaling less than 1");
}

const instances = appUrls.map((url) => ({ url: url.replace(/\/$/, ""), id: null }));
const operations = ["POST", "GET", "PUT"];
const statuses = new Map();
let networkErrors = 0;
let unexpectedStatuses = 0;
let inFlight = 0;
let sequence = 0;
let budget = 0;

async function seed(instance) {
  const response = await fetch(`${instance.url}/items`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ name: "load-seed" }),
    signal: AbortSignal.timeout(5000),
  });
  if (response.status !== 201) {
    throw new Error(`Could not seed ${instance.url}: HTTP ${response.status}`);
  }
  const item = await response.json();
  if (!Number.isSafeInteger(item.id) || item.id < 1) {
    throw new Error(`Could not seed ${instance.url}: invalid item ID`);
  }
  instance.id = item.id;
}

async function request(instance, operation, failure) {
  const isNotFound = failure === "404";
  const path = isNotFound ? "/items/2147483647" : operation === "POST" ? "/items" : `/items/${instance.id}`;
  const method = isNotFound ? "GET" : operation;
  const headers = {};
  if (failure === "500") {
    headers["x-demo-failure"] = "1";
  }
  if (!isNotFound && (operation === "POST" || operation === "PUT")) {
    headers["content-type"] = "application/json";
  }
  const response = await fetch(`${instance.url}${path}`, {
    method,
    headers,
    body: !isNotFound && (operation === "POST" || operation === "PUT")
      ? JSON.stringify({ name: `load-${sequence}` })
      : undefined,
    signal: AbortSignal.timeout(5000),
  });

  statuses.set(response.status, (statuses.get(response.status) ?? 0) + 1);
  const expected = failure === "404" ? 404 : failure === "500" ? 500 : operation === "POST" ? 201 : 200;
  if (response.status !== expected) {
    unexpectedStatuses++;
  }

  if (operation === "POST" && failure === null && response.status === 201) {
    const item = await response.json();
    if (Number.isSafeInteger(item.id) && item.id > 0) {
      instance.id = item.id;
    }
  } else {
    await response.arrayBuffer();
  }
}

function report() {
  const counts = [...statuses.entries()].sort(([a], [b]) => a - b)
    .map(([status, count]) => `${status}=${count}`).join(" ");
  console.log(`Last 30s: ${counts || "no responses"}; network errors=${networkErrors}; unexpected statuses=${unexpectedStatuses}`);
  statuses.clear();
  networkErrors = 0;
  unexpectedStatuses = 0;
}

async function main() {
  await Promise.all(instances.map(seed));
  console.log(`Load started: ramp to ${targetRps} RPS over ${rampSeconds}s across ${instances.length} instances`);

  const startedAt = performance.now();
  let previousTick = startedAt;
  const tick = setInterval(() => {
    const now = performance.now();
    const elapsedSeconds = (now - startedAt) / 1000;
    const deltaSeconds = Math.min((now - previousTick) / 1000, 1);
    previousTick = now;
    const currentRps = targetRps * Math.min(elapsedSeconds / rampSeconds, 1);
    budget = Math.min(budget + currentRps * deltaSeconds, targetRps);

    while (budget >= 1 && inFlight < 100) {
      budget--;
      const operation = operations[sequence % operations.length];
      const instance = instances[Math.floor(sequence / operations.length) % instances.length];
      sequence++;
      const choice = Math.random();
      const failure = choice < notFoundRate ? "404" : choice < notFoundRate + serverErrorRate ? "500" : null;
      inFlight++;
      void request(instance, operation, failure)
        .catch((error) => {
          networkErrors++;
          console.error(`Request to ${instance.url} failed:`, error);
        })
        .finally(() => { inFlight--; });
    }
  }, 100);
  const reporter = setInterval(report, 30_000);

  function stop() {
    clearInterval(tick);
    clearInterval(reporter);
    report();
  }
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
