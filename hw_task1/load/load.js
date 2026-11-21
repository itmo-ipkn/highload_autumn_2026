import http from 'k6/http';
import { check } from 'k6';

const targets = (__ENV.TARGETS || '').split(',').map((s) => s.trim()).filter(Boolean);
const allowed = /^http:\/\/(app-\d+|localhost|127\.0\.0\.1)(:\d+)?$/;
if (targets.length === 0 || !targets.every((t) => allowed.test(t))) {
  throw new Error('TARGETS must be a comma-separated list of local app URLs, e.g. http://app-1:8080,http://app-2:8080');
}

const targetRPS = Number(__ENV.TARGET_RPS || 40);
const startRPS = Number(__ENV.START_RPS || 5);
const rampUp = __ENV.RAMP_UP || '3m';
const missingRatio = Number(__ENV.MISSING_RATIO || 0.04);
if (!(targetRPS > 0 && targetRPS <= 200 && startRPS > 0 && startRPS <= targetRPS)) {
  throw new Error('need 0 < START_RPS <= TARGET_RPS <= 200');
}
if (!(missingRatio >= 0 && missingRatio <= 0.5)) throw new Error('MISSING_RATIO must be in [0, 0.5]');

const seedItems = 20;
const missingID = 999999999;
const maxLocalIDs = 1000;

export const options = {
  scenarios: {
    background: {
      executor: 'ramping-arrival-rate',
      startRate: startRPS,
      timeUnit: '1s',
      preAllocatedVUs: 10,
      maxVUs: 50,
      stages: [
        { target: targetRPS, duration: rampUp },
        { target: targetRPS, duration: '24h' },
      ],
    },
  },
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
};

const params = (name, extra = {}) => ({
  headers: { 'Content-Type': 'application/json' },
  tags: { name },
  timeout: '3s',
  ...extra,
});

const itemBody = (prefix) => JSON.stringify({ name: `${prefix}-${Date.now()}`, description: 'k6' });

function createItem(base) {
  const r = http.post(`${base}/items`, itemBody('item'), params('POST /items', { responseType: 'text' }));
  if (r.status !== 201) return null;
  try {
    return r.json().id;
  } catch (_) {
    return null;
  }
}

export function setup() {
  const ids = {};
  for (const base of targets) {
    ids[base] = [];
    for (let i = 0; i < seedItems; i++) {
      const id = createItem(base);
      if (id) ids[base].push(id);
    }
    if (ids[base].length === 0) throw new Error(`could not seed items on ${base}`);
  }
  return ids;
}

const localIDs = {};

function pickID(base, seeded) {
  if (Math.random() < missingRatio) return missingID;
  const pool = seeded[base].concat(localIDs[base] || []);
  return pool[Math.floor(Math.random() * pool.length)];
}

export default function (seeded) {
  const base = targets[Math.floor(Math.random() * targets.length)];
  const op = Math.random();

  if (op < 0.3) {
    const id = createItem(base);
    if (id) {
      const list = (localIDs[base] = localIDs[base] || []);
      list.push(id);
      if (list.length > maxLocalIDs) list.shift();
    }
    return;
  }

  const id = pickID(base, seeded);
  if (op < 0.7) {
    const r = http.get(`${base}/items/${id}`, params('GET /items/{id}'));
    check(r, { 'GET 200/404/500': (x) => [200, 404, 500].includes(x.status) });
  } else {
    const r = http.put(`${base}/items/${id}`, itemBody('updated'), params('PUT /items/{id}'));
    check(r, { 'PUT 200/404/500': (x) => [200, 404, 500].includes(x.status) });
  }
}
