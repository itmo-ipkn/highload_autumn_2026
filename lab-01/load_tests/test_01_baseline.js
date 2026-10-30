import http from 'k6/http';
import { check } from 'k6';

const APP1_BASE_URL = __ENV.APP1_BASE_URL || 'http://app1:8080';
const APP2_BASE_URL = __ENV.APP2_BASE_URL || 'http://app2:8080';
const FAKE_ID = "999999-non-existent-id";

// Helper: pre-create items in a specific app
function createItems(baseUrl, count = 100) {
  const ids = [];
  for (let i = 0; i < count; i++) {
    const payload = JSON.stringify({ value: `item-${i}` });
    const res = http.post(`${baseUrl}/items`, payload, {
      headers: { 'Content-Type': 'application/json' },
    });
    if (res.status === 201) {
      ids.push(JSON.parse(res.body).id);
    }
  }
  return ids;
}

// setup() runs ONCE before any VUs start. Seeds BOTH apps.
export function setup() {
  const app1Ids = createItems(APP1_BASE_URL);
  const app2Ids = createItems(APP2_BASE_URL);
  console.log(`Setup complete: app1=${app1Ids.length} items, app2=${app2Ids.length} items`);
  return { app1Ids, app2Ids };
}

// Shared ramp-up profile: exactly 60 seconds (1 minute) total
function makeStages() {
  return [
    { target: 20, duration: '15s' },  // 5 -> 20 RPS in 15s
    { target: 40, duration: '45s' },  // 20 -> 40 RPS in 45s (Total: 60s)
  ];
}

// Core load test logic, parameterized by target URL and item pool
function runLoadTest(baseUrl, itemIds) {
  const rand = Math.random();

  if (rand < 0.30) {
    // POST /items - VALID (30% of traffic)
    const payload = JSON.stringify({
      value: `item-${Date.now()}-${Math.floor(Math.random() * 1000)}`,
    });
    const res = http.post(`${baseUrl}/items`, payload, {
      headers: { 'Content-Type': 'application/json' },
    });
    check(res, { 'POST /items (valid) status is 201': (r) => r.status === 201 });

  } else if (rand < 0.38) {
    // POST /items - INVALID PAYLOAD (8% of traffic)
    const invalidPayload = JSON.stringify("just a string, not an object");
    const res = http.post(`${baseUrl}/items`, invalidPayload, {
      headers: { 'Content-Type': 'application/json' },
    });
    check(res, { 'POST /items (invalid) status is 400': (r) => r.status === 400 });

  } else if (rand < 0.71) {
    // GET /items/{id} (33% of traffic)
    // 80% valid ID -> 200, 20% invalid ID -> 404
    const isInvalid = Math.random() < 0.2;
    const id = isInvalid
      ? FAKE_ID
      : itemIds[Math.floor(Math.random() * itemIds.length)];
    const res = http.get(`${baseUrl}/items/${id}`);
    if (isInvalid) {
      check(res, { 'GET /items/{id} status is 404': (r) => r.status === 404 });
    } else {
      check(res, { 'GET /items/{id} status is 200': (r) => r.status === 200 });
    }

  } else {
    // PUT /items/{id} (29% of traffic)
    // 80% valid ID -> 200, 20% invalid ID -> 404
    const isInvalid = Math.random() < 0.2;
    const id = isInvalid
      ? FAKE_ID
      : itemIds[Math.floor(Math.random() * itemIds.length)];
    const payload = JSON.stringify({
      value: `updated-${Date.now()}-${Math.floor(Math.random() * 1000)}`,
    });
    const res = http.put(`${baseUrl}/items/${id}`, payload, {
      headers: { 'Content-Type': 'application/json' },
    });
    if (isInvalid) {
      check(res, { 'PUT /items/{id} status is 404': (r) => r.status === 404 });
    } else {
      check(res, { 'PUT /items/{id} status is 200': (r) => r.status === 200 });
    }
  }
}

// Two scenarios, each running independently against one app
export const options = {
  scenarios: {
    app1_load: {
      executor: 'ramping-arrival-rate',
      startRate: 5,
      timeUnit: '1s',
      preAllocatedVUs: 50,
      maxVUs: 200,
      stages: makeStages(),
      exec: 'app1Default',
    },
    app2_load: {
      executor: 'ramping-arrival-rate',
      startRate: 5,
      timeUnit: '1s',
      preAllocatedVUs: 50,
      maxVUs: 200,
      stages: makeStages(),
      exec: 'app2Default',
    },
  },
};

// Scenario entry points
export function app1Default(data) {
  runLoadTest(APP1_BASE_URL, data.app1Ids);
}

export function app2Default(data) {
  runLoadTest(APP2_BASE_URL, data.app2Ids);
}

// Fallback default (required by k6, though scenarios override it)
export default function (data) {
  runLoadTest(APP1_BASE_URL, data.app1Ids);
}
