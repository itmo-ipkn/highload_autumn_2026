import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";

// Exercise the pinned dependency's actual scheduling path, not a copied formula.
const require = createRequire(import.meta.url);
const RequestQueue = require("kafkajs/src/network/requestQueue/index.js");
function makeQueue() {
  return new RequestQueue({
    maxInFlightRequests: 1,
    requestTimeout: 1000,
    enforceRequestTimeout: false,
    clientId: "timer-regression",
    broker: "unused:9092",
    logger: { debug() {}, warn() {} },
  });
}

test("KafkaJS idle request queue never schedules a negative or recurring timer", (t) => {
  const delays: number[] = [];
  t.mock.method(globalThis, "setTimeout", (_callback: () => void, delay: number) => {
    delays.push(delay);
    return { unref() {} };
  });
  const queue = makeQueue();
  try {
    queue.checkPendingRequests();
    assert.deepEqual(delays, [], "An idle queue must not schedule a timeout from throttledUntil=-1");
  } finally { queue.destroy(); }
});

function request(correlationId: number) {
  return {
    entry: { correlationId, resolve() {}, reject() {} },
    expectResponse: true,
    sendRequest() {},
  };
}

test("KafkaJS completion drains pending requests and stops idle timer rescheduling", (t) => {
  const scheduled: { callback: () => void; delay: number }[] = [];
  t.mock.method(globalThis, "setTimeout", (callback: () => void, delay: number) => {
    scheduled.push({ callback, delay });
    return { unref() {} };
  });
  const queue = makeQueue();
  try {
    queue.push(request(1));
    queue.push(request(2));
    assert.equal(scheduled.length, 1);
    assert.equal(scheduled[0]!.delay, 10);
    queue.fulfillRequest({ correlationId: 1, size: 0, payload: null });
    assert.equal(queue.pending.length, 0);
    assert.equal(queue.inflight.size, 1);
    queue.fulfillRequest({ correlationId: 2, size: 0, payload: null });
    scheduled[0]!.callback();
    assert.equal(scheduled.length, 1, "An expired timer must not restart on an idle queue");
    assert.equal(queue.throttleCheckTimeoutId, null);
  } finally { queue.destroy(); }
});

test("KafkaJS computes remaining throttle duration and positive delay for expired throttles", (t) => {
  let now = 1000;
  t.mock.method(Date, "now", () => now);
  const scheduled: { callback: () => void; delay: number }[] = [];
  t.mock.method(globalThis, "setTimeout", (callback: () => void, delay: number) => {
    scheduled.push({ callback, delay });
    return { unref() {} };
  });
  const queue = makeQueue();
  try {
    queue.maybeThrottle(50);
    now = 1020;
    queue.push(request(1));
    assert.equal(scheduled[0]!.delay, 30);
    now = 1050;
    scheduled[0]!.callback();
    assert.equal(queue.pending.length, 0);
    assert.equal(queue.inflight.size, 1);
    queue.push(request(2));
    assert.equal(scheduled[1]!.delay, 10);
    assert.ok(scheduled.every(({ delay }) => Number.isFinite(delay) && delay > 0));
  } finally { queue.destroy(); }
});
