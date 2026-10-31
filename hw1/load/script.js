// Сценарий нагрузки k6 для HW-1.
//
// Источник истины по цифрам и правилам — docs/load-contract.md: интенсивность
// задаётся в итерациях в секунду, вес итерации пересчитывается в запросы/с,
// каждая итерация привязана к одному поду.
//
// Запуск (в compose k6 поднимается автоматически):
//   k6 run load/script.js
// Отладочный короткий прогон:
//   K6_HOLD=2m k6 run load/script.js

import http from 'k6/http';
import { check } from 'k6';

import {
  BUSINESS_STATUSES,
  EXPECTED_STATUSES,
  GRACEFUL_STOP,
  MAX_VUS,
  MISSING_STATUSES,
  PRE_ALLOCATED_VUS,
  buildStages,
  holdDuration,
  isMissingIdIteration,
  itemPayload,
  iterationSequence,
  missingItemId,
  pickPodIndex,
  podTag,
} from './lib.js';

const PODS = [__ENV.APP1_URL || 'http://app-1:8000', __ENV.APP2_URL || 'http://app-2:8000'];
const HOLD = holdDuration(__ENV);

http.setResponseCallback(http.expectedStatuses(...EXPECTED_STATUSES));

export const options = {
  scenarios: {
    load: {
      executor: 'ramping-arrival-rate',
      startRate: 2,
      timeUnit: '1s',
      stages: buildStages(HOLD),
      preAllocatedVUs: PRE_ALLOCATED_VUS,
      maxVUs: MAX_VUS,
      gracefulStop: GRACEFUL_STOP,
    },
  },
  // Только пороги про сам инструмент: интенсивность проверяется метриками
  // приложения (docs/load-contract.md §2), иначе просадка сети даёт ложный exit 99.
  thresholds: {
    dropped_iterations: ['rate<0.01'],
    checks: ['rate>0.99'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'max'],
};

const JSON_HEADERS = { 'Content-Type': 'application/json' };

function expected(response, what, allowed) {
  check(response, {
    [`${what}: ожидаемый статус`]: (res) => allowed.includes(res.status),
  });
}

function createdId(response) {
  try {
    return response.json('id');
  } catch (error) {
    return undefined;
  }
}

export default function () {
  const podIndex = pickPodIndex(__VU, __ITER, PODS.length);
  const base = PODS[podIndex];
  const tags = podTag(podIndex);
  const sequence = iterationSequence(__VU, __ITER);

  const created = http.post(`${base}/items`, JSON.stringify(itemPayload(sequence)), {
    headers: JSON_HEADERS,
    tags,
  });
  expected(created, 'POST /items', BUSINESS_STATUSES);

  if (created.status !== 200) {
    return;
  }

  const id = createdId(created);
  check(created, { 'POST /items: id в ответе': () => typeof id === 'string' && id.length > 0 });
  if (!id) {
    return;
  }

  expected(http.get(`${base}/items/${id}`, { tags }), 'GET /items/{id}', BUSINESS_STATUSES);
  expected(
    http.put(`${base}/items/${id}`, JSON.stringify(itemPayload(sequence + 1)), {
      headers: JSON_HEADERS,
      tags,
    }),
    'PUT /items/{id}',
    BUSINESS_STATUSES,
  );

  if (isMissingIdIteration(__ITER)) {
    expected(
      http.get(`${base}/items/${missingItemId(sequence)}`, { tags }),
      'GET /items/{отсутствующий id}',
      MISSING_STATUSES,
    );
  }
}

export function handleSummary(data) {
  const value = (name, field, fallback) => {
    const metric = data.metrics[name];
    if (!metric || !metric.values) {
      return fallback;
    }
    const raw = metric.values[field];
    return raw === undefined ? fallback : raw;
  };

  const line = (label, number, digits = 2) =>
    `${label}: ${typeof number === 'number' ? number.toFixed(digits) : number}`;

  return {
    stdout: [
      '',
      '=== HW-1 k6 summary (docs/load-contract.md) ===',
      line('итераций выполнено', value('iterations', 'count', 'n/a'), 0),
      line('итераций потеряно, доля', value('dropped_iterations', 'rate', 0), 4),
      line('запросов выполнено', value('http_reqs', 'count', 'n/a'), 0),
      line('checks прошли, доля', value('checks', 'rate', 'n/a'), 4),
      line('p95 длительности запроса, мс', value('http_req_duration', 'p(95)', 'n/a'), 1),
      '',
      'Интенсивность и доли ошибок проверяются в Prometheus, а не здесь:',
      '  sum(rate(http_requests_total{handler!="unmatched"}[60s]))',
      '',
    ].join('\n'),
  };
}
