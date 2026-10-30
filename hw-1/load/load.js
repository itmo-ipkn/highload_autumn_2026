import http from 'k6/http';

const TARGETS = __ENV.TARGETS.split(',');
const SEED_ITEMS = 50;
const NOT_FOUND_SHARE = 0.03;
const JSON_BODY = { headers: { 'Content-Type': 'application/json' } };

export const options = {
  discardResponseBodies: true,
  scenarios: {
    background: {
      executor: 'ramping-arrival-rate',
      startRate: 1,
      timeUnit: '1s',
      preAllocatedVUs: 10,
      maxVUs: 50,
      stages: [
        { target: 30, duration: '3m' },
        { target: 30, duration: '24h' },
      ],
    },
  },
};

export function setup() {
  const ids = {};
  for (const target of TARGETS) {
    ids[target] = [];
    while (ids[target].length < SEED_ITEMS) {
      const res = http.post(`${target}/items`, JSON.stringify({ name: 'seed' }),
        Object.assign({ responseType: 'text' }, JSON_BODY));
      if (res.status === 201) {
        ids[target].push(res.json('id'));
      }
    }
  }
  return ids;
}

export default function (ids) {
  const target = pick(TARGETS);
  const id = Math.random() < NOT_FOUND_SHARE ? 1000000000 + Math.floor(Math.random() * 1000) : pick(ids[target]);

  const roll = Math.random();
  if (roll < 0.2) {
    http.post(`${target}/items`, JSON.stringify({ name: 'item' }), JSON_BODY);
  } else if (roll < 0.7) {
    http.get(`${target}/items/${id}`);
  } else {
    http.put(`${target}/items/${id}`, JSON.stringify({ name: 'updated' }), JSON_BODY);
  }
}

function pick(values) {
  return values[Math.floor(Math.random() * values.length)];
}
