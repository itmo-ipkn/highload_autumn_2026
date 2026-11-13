import http from 'k6/http';

const TARGETS = (__ENV.TARGETS || 'http://app-1:8000,http://app-2:8000').split(',');
const RPS = Number(__ENV.TARGET_RPS || 40);
const NOT_FOUND_SHARE = 0.03;

export const options = {
  scenarios: {
    background: {
      executor: 'ramping-arrival-rate',
      startRate: 1,
      timeUnit: '1s',
      preAllocatedVUs: 20,
      maxVUs: 100,
      stages: [
        { duration: '1m', target: Math.round(RPS / 4) },
        { duration: '1m', target: Math.round(RPS / 2) },
        { duration: '1m', target: RPS },
        { duration: '1000h', target: RPS },
      ],
    },
  },
  summaryTrendStats: ['p(50)', 'p(95)', 'p(99)'],
};

http.setResponseCallback(http.expectedStatuses({ min: 200, max: 299 }, 404, 500));

const JSON_HEADERS = { headers: { 'Content-Type': 'application/json' } };
const known = Object.fromEntries(TARGETS.map((t) => [t, []]));

function item() {
  return JSON.stringify({ name: `item-${Math.floor(Math.random() * 1e6)}`, price: Math.round(Math.random() * 1e4) / 100 });
}

function create(base) {
  const res = http.post(`${base}/items`, item(), { ...JSON_HEADERS, tags: { name: 'POST /items' } });
  if (res.status === 201) known[base].push(res.json('id'));
}

export default function () {
  const base = TARGETS[Math.floor(Math.random() * TARGETS.length)];
  const ids = known[base];
  const r = Math.random();

  if (ids.length === 0 || r < 0.3) {
    create(base);
    return;
  }
  const missing = Math.random() < NOT_FOUND_SHARE;
  const id = missing ? 1e9 + Math.floor(Math.random() * 1e6) : ids[Math.floor(Math.random() * ids.length)];
  if (r < 0.75) {
    http.get(`${base}/items/${id}`, { tags: { name: 'GET /items/{id}' } });
  } else {
    http.put(`${base}/items/${id}`, item(), { ...JSON_HEADERS, tags: { name: 'PUT /items/{id}' } });
  }
}
