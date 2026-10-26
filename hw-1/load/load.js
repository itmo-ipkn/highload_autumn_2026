// Фоновая нагрузка: плавный разгон до RATE запросов в секунду на оба экземпляра,
// затем полка до остановки стека. Каждая итерация - один запрос.
import http from 'k6/http';

const TARGETS = (__ENV.TARGETS || 'http://localhost:8081,http://localhost:8082').split(',');
const RATE = Number(__ENV.RATE || 30);
const RAMP = __ENV.RAMP || '3m';
// доля GET/PUT к несуществующим id - источник 404
const MISS_RATE = Number(__ENV.MISS_RATE || 0.05);

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
        { target: RATE, duration: RAMP },
        { target: RATE, duration: '24h' },
      ],
    },
  },
};

const params = (name) => ({
  headers: { 'Content-Type': 'application/json' },
  tags: { name },
  responseCallback: http.expectedStatuses({ min: 200, max: 499 }),
});

// id, созданные этим VU, отдельно по экземплярам: хранилища у них независимые
const created = Object.fromEntries(TARGETS.map((t) => [t, []]));

function pickId(target) {
  const ids = created[target];
  if (ids.length === 0 || Math.random() < MISS_RATE) {
    return 1e9 + Math.floor(Math.random() * 1e6);
  }
  return ids[Math.floor(Math.random() * ids.length)];
}

function createItem(target) {
  const body = JSON.stringify({ name: `item-${__VU}-${__ITER}`, value: String(Math.random()) });
  const res = http.post(`${target}/items`, body, { ...params('POST /items'), responseType: 'text' });
  if (res.status === 201) {
    created[target].push(res.json('id'));
  }
}

export default function () {
  const target = TARGETS[Math.floor(Math.random() * TARGETS.length)];
  const r = Math.random();

  if (r < 0.3 || created[target].length === 0) {
    createItem(target);
  } else if (r < 0.8) {
    http.get(`${target}/items/${pickId(target)}`, params('GET /items/{id}'));
  } else {
    const body = JSON.stringify({ name: 'updated', value: String(Math.random()) });
    http.put(`${target}/items/${pickId(target)}`, body, params('PUT /items/{id}'));
  }
}
