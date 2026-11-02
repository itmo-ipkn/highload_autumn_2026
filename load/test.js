import http from 'k6/http';
import { check } from 'k6';

const APP1 = (__ENV.APP1_URL || 'http://localhost:8080').replace(/\/$/, '');
const APP2 = (__ENV.APP2_URL || 'http://localhost:8081').replace(/\/$/, '');
const TARGET_RPS_PER_POD = Math.max(1, Number(__ENV.TARGET_RPS_PER_POD || 25));

// Two independent arrival-rate scenarios run in parallel:
// each pod ramps from 1 to 25 RPS in 2m, then stays at 25 RPS for 28m.
// Total test window is exactly 30m; aggregate target is 50 RPS.
const stages = [
  { target: TARGET_RPS_PER_POD, duration: '2m' },
  { target: TARGET_RPS_PER_POD, duration: '28m' },
];

export const options = {
  scenarios: {
    app1_traffic: {
      executor: 'ramping-arrival-rate',
      exec: 'app1Traffic',
      startRate: 1,
      timeUnit: '1s',
      preAllocatedVUs: 20,
      maxVUs: 80,
      stages,
      gracefulStop: '0s',
    },
    app2_traffic: {
      executor: 'ramping-arrival-rate',
      exec: 'app2Traffic',
      startRate: 1,
      timeUnit: '1s',
      preAllocatedVUs: 20,
      maxVUs: 80,
      stages,
      gracefulStop: '0s',
    },
  },
  thresholds: {
    // HTTP 404 and injected HTTP 500 responses are expected by this exercise.
    http_req_duration: ['p(95)<3000'],
  },
};

const jsonParams = {
  headers: { 'Content-Type': 'application/json' },
  timeout: '3s',
};

export function app1Traffic() {
  runTraffic(APP1);
}

export function app2Traffic() {
  runTraffic(APP2);
}

function runTraffic(base) {
  const choice = Math.random();

  if (choice < 0.40) {
    const response = http.post(
      `${base}/items`,
      JSON.stringify({
        name: `item-vu-${__VU}-${__ITER}`,
        description: 'created by k6 background load',
      }),
      jsonParams,
    );
    check(response, {
      'POST /items returns 201 or injected 500': (r) => r.status === 201 || r.status === 500,
    });
    if (response.status === 201) {
      try {
        const body = response.json();
        if (body && body.id !== undefined) __VU_ITEM_ID = body.id;
      } catch (_) {
        // A later iteration can create another item.
      }
    }
    return;
  }

  if (choice < 0.75) {
    const id = (typeof __VU_ITEM_ID === 'undefined') ? 999999999 : __VU_ITEM_ID;
    const response = http.get(`${base}/items/${id}`, { timeout: '3s' });
    check(response, {
      'GET /items/{id} returns an expected status': (r) => [200, 404, 500].includes(r.status),
    });
    return;
  }

  if (choice < 0.95) {
    if (typeof __VU_ITEM_ID === 'undefined') {
      const response = http.get(`${base}/items/999999999`, { timeout: '3s' });
      check(response, {
        'GET missing item returns 404 or injected 500': (r) => r.status === 404 || r.status === 500,
      });
      return;
    }
    const response = http.put(
      `${base}/items/${__VU_ITEM_ID}`,
      JSON.stringify({ name: `updated-vu-${__VU}`, description: 'updated by k6 background load' }),
      jsonParams,
    );
    check(response, {
      'PUT /items/{id} returns an expected status': (r) => [200, 404, 500].includes(r.status),
    });
    return;
  }

  // Deliberately generate a deterministic 404 path (unless the server injects a 500 first).
  const response = http.get(`${base}/items/999999999`, { timeout: '3s' });
  check(response, {
    'GET nonexistent ID returns 404 or injected 500': (r) => r.status === 404 || r.status === 500,
  });
}

// k6 isolates this state per VU; each VU keeps its own created item ID.
let __VU_ITEM_ID;
