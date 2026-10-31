// Чистая логика сценария нагрузки: без импортов k6 и без сети.
//
// Модуль существует отдельно от script.js, чтобы его можно было прогонять
// в Node (`node --test load/tests/`): k6 в рабочем окружении нет, а арифметику
// интенсивности и привязку к поду надо доказать исполнением, а не чтением.
// Источник истины по цифрам — docs/load-contract.md.

export const TARGET_ITER_PER_SECOND = 12;
export const START_RATE = 2;
export const RAMP_MIDDLE_RATE = 5;
export const RAMP_UP_SECONDS = 60;
export const RAMP_TO_TARGET_SECONDS = 120;

export const DEFAULT_HOLD = '12h';
export const PRE_ALLOCATED_VUS = 20;
export const MAX_VUS = 100;
export const GRACEFUL_STOP = '5s';

export const ITERATION_REQUESTS = 3;
export const EXTRA_404_EVERY = 6;

// Доля итераций, которые обрываются после 500 на POST (инъекция в приложении).
export const FAILURE_RATE = 0.03;

export const EXPECTED_STATUSES = [200, 404, 500];

// Разрешённые ответы по видам запросов: 404 штатен только для запроса
// отсутствующего id. Иначе 404 на POST (неверный URL или путь) прошёл бы checks
// и «нагрузка в пустоту» выглядела бы успехом.
export const BUSINESS_STATUSES = [200, 500];
export const MISSING_STATUSES = [404, 500];

const HOLD_PATTERN = /^[1-9]\d*[smh]$/;

export function holdDuration(env = {}) {
  const raw = env.K6_HOLD;
  if (raw === undefined || raw === '') {
    return DEFAULT_HOLD;
  }
  if (!HOLD_PATTERN.test(String(raw))) {
    throw new Error(`K6_HOLD должен быть вида "12h"/"30m"/"90s", получено: ${raw}`);
  }
  return String(raw);
}

export function buildStages(hold = DEFAULT_HOLD) {
  return [
    { target: RAMP_MIDDLE_RATE, duration: `${RAMP_UP_SECONDS}s` },
    { target: TARGET_ITER_PER_SECOND, duration: `${RAMP_TO_TARGET_SECONDS}s` },
    { target: TARGET_ITER_PER_SECOND, duration: hold },
  ];
}

export function pickPodIndex(vu, iter, podCount) {
  if (!Number.isInteger(podCount) || podCount < 1) {
    throw new Error(`нужен хотя бы один под, получено: ${podCount}`);
  }
  return (vu + iter) % podCount;
}

export function isMissingIdIteration(iter, everyNth = EXTRA_404_EVERY) {
  if (!Number.isInteger(everyNth) || everyNth < 2) {
    throw new Error(`everyNth должен быть >= 2, получено: ${everyNth}`);
  }
  return (iter + 1) % everyNth === 0;
}

// Ожидаемое число запросов на итерацию: обрыв после 500 на POST убирает
// два зависимых запроса и дополнительный 404-запрос.
export function requestsPerIteration(everyNth = EXTRA_404_EVERY, failureRate = FAILURE_RATE) {
  const full = ITERATION_REQUESTS + 1 / everyNth;
  return failureRate + (1 - failureRate) * full;
}

export function expectedRequestsPerSecond(iterPerSecond = TARGET_ITER_PER_SECOND, everyNth = EXTRA_404_EVERY, failureRate = FAILURE_RATE) {
  return iterPerSecond * requestsPerIteration(everyNth, failureRate);
}

// Сквозной номер итерации: имена записей не повторяются между подами и итерациями.
export function iterationSequence(vu, iter) {
  return vu * 1_000_000 + iter;
}

export function itemPayload(sequence) {
  return { name: `k6-${sequence}`, value: sequence % 1_000_000 };
}

export function missingItemId(sequence) {
  return `k6-missing-${sequence}`;
}

export function podTag(podIndex) {
  return { pod: `pod-${podIndex + 1}` };
}
