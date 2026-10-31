// Тесты чистой логики сценария. Запуск: node --test load/tests/ (или npm test в load/).
// Ожидаемые числа взяты из docs/load-contract.md, а не из кода: иначе тест не может упасть.

import assert from 'node:assert/strict';
import test from 'node:test';

import {
  BUSINESS_STATUSES,
  DEFAULT_HOLD,
  EXPECTED_STATUSES,
  FAILURE_RATE,
  MAX_VUS,
  MISSING_STATUSES,
  PRE_ALLOCATED_VUS,
  TARGET_ITER_PER_SECOND,
  buildStages,
  expectedRequestsPerSecond,
  holdDuration,
  isMissingIdIteration,
  itemPayload,
  iterationSequence,
  missingItemId,
  pickPodIndex,
  podTag,
  requestsPerIteration,
} from '../lib.js';

const closeTo = (actual, expected, epsilon = 1e-6) => {
  assert.ok(Math.abs(actual - expected) < epsilon, `${actual} != ${expected} (±${epsilon})`);
};

test('вес итерации учитывает обрыв после 500 на POST', () => {
  // 0.03 * 1 + 0.97 * (3 + 1/6)
  closeTo(requestsPerIteration(), 3.1016666667);
});

test('без инъекции 500 вес итерации равен 3 + 1/6', () => {
  closeTo(requestsPerIteration(6, 0), 3.1666666667);
});

test('целевая интенсивность даёт 20–50 req/s', () => {
  const rps = expectedRequestsPerSecond(TARGET_ITER_PER_SECOND);

  closeTo(rps, 37.22, 0.01);
  assert.ok(rps >= 20 && rps <= 50, `${rps} вне коридора L2`);
});

test('стартовая интенсивность рампы остаётся в нижней части коридора', () => {
  const rps = expectedRequestsPerSecond(2);

  closeTo(rps, 6.2, 0.01);
});

test('константы сценария соответствуют контракту', () => {
  assert.equal(TARGET_ITER_PER_SECOND, 12);
  assert.equal(PRE_ALLOCATED_VUS, 20);
  assert.equal(MAX_VUS, 100);
  assert.equal(DEFAULT_HOLD, '12h');
  assert.equal(FAILURE_RATE, 0.03);
  assert.deepEqual(EXPECTED_STATUSES, [200, 404, 500]);
});

test('404 разрешён только для запроса отсутствующего id', () => {
  assert.deepEqual(BUSINESS_STATUSES, [200, 500]);
  assert.deepEqual(MISSING_STATUSES, [404, 500]);
});

test('общий список ожидаемых статусов — объединение частных', () => {
  const union = [...new Set([...BUSINESS_STATUSES, ...MISSING_STATUSES])].sort((a, b) => a - b);

  assert.deepEqual(union, EXPECTED_STATUSES);
});

test('под выбирается по чётности VU и итерации', () => {
  assert.equal(pickPodIndex(0, 0, 2), 0);
  assert.equal(pickPodIndex(0, 1, 2), 1);
  assert.equal(pickPodIndex(1, 0, 2), 1);
  assert.equal(pickPodIndex(1, 1, 2), 0);
});

test('выбор пода детерминирован внутри итерации', () => {
  const first = pickPodIndex(7, 3, 2);

  for (let attempt = 0; attempt < 5; attempt += 1) {
    assert.equal(pickPodIndex(7, 3, 2), first);
  }
});

test('соседние итерации чередуют поды', () => {
  const pods = [0, 1, 2, 3].map((iter) => pickPodIndex(5, iter, 2));

  assert.deepEqual(pods, [1, 0, 1, 0]);
});

test('выбор пода без подов — ошибка', () => {
  assert.throws(() => pickPodIndex(0, 0, 0), /хотя бы один под/);
});

test('404-запрос делается на каждой шестой итерации', () => {
  const flags = Array.from({ length: 12 }, (_, iter) => isMissingIdIteration(iter));

  assert.deepEqual(flags, [false, false, false, false, false, true, false, false, false, false, false, true]);
});

test('некорректный шаг 404-итерации — ошибка', () => {
  assert.throws(() => isMissingIdIteration(0, 1), />= 2/);
});

test('hold по умолчанию 12h, переопределяется через K6_HOLD', () => {
  assert.equal(holdDuration({}), '12h');
  assert.equal(holdDuration({ K6_HOLD: '' }), '12h');
  assert.equal(holdDuration({ K6_HOLD: '2m' }), '2m');
  assert.equal(holdDuration({ K6_HOLD: '90s' }), '90s');
});

test('некорректный K6_HOLD отвергается', () => {
  for (const bad of ['2 minutes', '12', '0s', '-5m', 'h']) {
    assert.throws(() => holdDuration({ K6_HOLD: bad }), /K6_HOLD/, `принят ${bad}`);
  }
});

test('стадии: две рампы по 60 и 120 секунд, затем hold', () => {
  assert.deepEqual(buildStages('2m'), [
    { target: 5, duration: '60s' },
    { target: 12, duration: '120s' },
    { target: 12, duration: '2m' },
  ]);
  assert.deepEqual(buildStages().at(-1), { target: 12, duration: '12h' });
});

test('сквозной номер итерации уникален для пары VU и итерации', () => {
  const seen = new Set();

  for (const vu of [0, 1, 2]) {
    for (const iter of [0, 1, 2, 999_999]) {
      seen.add(iterationSequence(vu, iter));
    }
  }
  assert.equal(seen.size, 12);
  assert.equal(iterationSequence(3, 7), 3_000_007);
});

test('тело POST проходит ограничения модели', () => {
  const payload = itemPayload(iterationSequence(999, 999_999));

  assert.ok(payload.name.length >= 1 && payload.name.length <= 100, payload.name);
  assert.ok(Number.isInteger(payload.value) && payload.value >= 0);
  assert.equal(itemPayload(10).name, 'k6-10');
});

test('id несуществующей записи не совпадает с реальным', () => {
  assert.equal(missingItemId(42), 'k6-missing-42');
  assert.ok(missingItemId(42).length <= 100);
});

test('тег пода отличает поды в отчёте k6', () => {
  assert.deepEqual(podTag(0), { pod: 'pod-1' });
  assert.deepEqual(podTag(1), { pod: 'pod-2' });
});
