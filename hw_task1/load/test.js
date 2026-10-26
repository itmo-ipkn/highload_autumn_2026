import http from 'k6/http';
import { check } from 'k6';

const TARGET_RPS = Number(__ENV.TARGET_RPS || 50);
const RAMP_UP = __ENV.RAMP_UP || '2m';
const HOLD = __ENV.HOLD || '30m';

// 50 RPS суммарно -> по 25 на каждый под.
const PER_POD_RPS = Math.floor(TARGET_RPS / 2);

const APP1 = __ENV.APP1_URL || 'http://app1:8080';
const APP2 = __ENV.APP2_URL || 'http://app2:8080';

// Сервер держит по одному keep-alive соединению на поток.
// Закрываем соединение после каждого запроса, чтобы /health не голодал.
const NO_KEEPALIVE = { 'Connection': 'close' };

const stages = [
  { target: PER_POD_RPS, duration: RAMP_UP },
  { target: PER_POD_RPS, duration: HOLD },
];

export const options = {
  scenarios: {
    app1: {
      executor: 'ramping-arrival-rate',
      exec: 'hitApp1',
      startRate: 1,
      timeUnit: '1s',
      preAllocatedVUs: 20,
      maxVUs: 40,
      stages,
    },
    app2: {
      executor: 'ramping-arrival-rate',
      exec: 'hitApp2',
      startRate: 1,
      timeUnit: '1s',
      preAllocatedVUs: 20,
      maxVUs: 40,
      stages,
    },
  },
};

function hit(base) {
  const roll = Math.random();

  if (roll < 0.40) {
    const res = http.post(
      `${base}/items`,
      JSON.stringify({ name: `load-${Date.now()}` }),
      { headers: { 'Content-Type': 'application/json', ...NO_KEEPALIVE } },
    );
    check(res, { 'POST accepted': (r) => [201, 500].includes(r.status) });
    return;
  }

  if (roll < 0.75) {
    const id = Math.floor(Math.random() * 50) + 1;
    const res = http.get(`${base}/items/${id}`, { headers: NO_KEEPALIVE });
    check(res, { 'GET accepted': (r) => [200, 404, 500].includes(r.status) });
    return;
  }

  if (roll < 0.95) {
    const id = Math.floor(Math.random() * 50) + 1;
    const res = http.put(
      `${base}/items/${id}`,
      JSON.stringify({ name: `upd-${Date.now()}` }),
      { headers: { 'Content-Type': 'application/json', ...NO_KEEPALIVE } },
    );
    check(res, { 'PUT accepted': (r) => [200, 404, 500].includes(r.status) });
    return;
  }

  const res = http.get(`${base}/items/999999`, { headers: NO_KEEPALIVE });
  check(res, { 'missing id is 404 or 500': (r) => [404, 500].includes(r.status) });
}

export function hitApp1() {
  hit(APP1);
}

export function hitApp2() {
  hit(APP2);
}