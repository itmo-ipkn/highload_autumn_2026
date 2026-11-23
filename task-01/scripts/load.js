import http from 'k6/http';
import { check } from 'k6';
import { sleep } from 'k6';

const BASE_URL = __ENV.TARGET_URL || 'http://crud-service:3001';

const error500Rate = 0.005;
const error404Rate = 0.03;
const error400Rate = 0.01;

const urls = [
  { path: '/items', method: 'POST', body: { name: 'test', description: 'load test' } },
  { path: '/items/1', method: 'GET' }, 
  { path: '/items/2', method: 'GET' },
  { path: '/items/1', method: 'PUT', body: { name: 'updated-test', description: 'updated' } },
];

export const options = {
  stages: [
    { duration: '1m', target: 10 },
    { duration: '2m', target: 30 },
    { duration: '3m', target: 50 },
    { duration: '10m', target: 50 },
  ],
};

export default function () {
  const randomUrl = urls[Math.floor(Math.random() * urls.length)];
  
  let query = '';
  const r = Math.random();
  if (r < error500Rate) {
    query = '?forceError=500';
  } else if (r < error500Rate + error404Rate) {
    // Для 404 можно подставить несуществующий ID
    query = '/999999';
  } else if (r < error500Rate + error404Rate + error400Rate) {
    // Для 400 — некорректный запрос (например, пустой POST или неверный JSON)
    query = '?invalid=true';
  }

  // Формируем итоговый URL: базовый путь + (для 404 — заменяем путь, иначе — добавляем query)
  let url;
  if (query === '/999999') {
    url = `${BASE_URL}${randomUrl.path.replace(/\/\d+$/, query)}`;
  } else {
    url = `${BASE_URL}${randomUrl.path}${query}`;
  }

  let resp;
  if (randomUrl.method === 'POST') {
    let payload = randomUrl.body;
    if (query.includes('?invalid=true')) {
      payload = {}; // невалидный payload для 400
    }
    resp = http.post(url, JSON.stringify(payload), {
      headers: { 'Content-Type': 'application/json' },
    });
  } else if (randomUrl.method === 'PUT') {
    let payload = randomUrl.body;
    if (query.includes('?invalid=true')) {
      payload = {};
    }
    resp = http.put(url, JSON.stringify(payload), {
      headers: { 'Content-Type': 'application/json' },
    });
  } else {
    resp = http.get(url);
  }

  check(resp, {
    'status is expected': (r) => {
      if (query.includes('?forceError=500')) return r.status === 500;
      if (query === '/999999') return r.status === 404;
      if (query.includes('?invalid=true')) return r.status === 400;
      return r.status === 200 || r.status === 500; // допускаем 200 и 500 на обычных запросах
    },
  });

  sleep(0.1);
}
