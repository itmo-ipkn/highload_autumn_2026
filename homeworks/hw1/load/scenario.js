// Continuous background load for both app instances.
//
// Compose runs this with grafana/k6:1.6.0 as PID 1:
//   k6 run --quiet --no-thresholds --summary-mode=disabled --address=
// duration "0s" on the externally-controlled executor means "until the
// process is stopped". LOAD_DURATION may select a finite run for CI; Compose
// leaves it unset. Do not replace this with a long finite stage, --linger,
// or a restart loop.
//
// --no-thresholds together with --summary-mode=disabled skips k6's metrics
// ingester. In 1.6.0 that ingester appends every trend sample for the end
// summary, so an infinite run would grow without bound. Checks still mark
// the test failed and log the first unexpected responses.

import http from 'k6/http';
import { check, sleep } from 'k6';
import exec from 'k6/execution';
import {
  VUS,
  advanceAfterRequest,
  planIteration,
  scheduleAt,
  targetRps,
} from './pace.js';

const SEED_NAME = 'seed';
const SEED_VALUE = 'seed';
const ITEM_NAME = 'n';
const ITEM_VALUE = 'v';
const SEED_BODY = JSON.stringify({ name: SEED_NAME, value: SEED_VALUE });
const ITEM_BODY = JSON.stringify({ name: ITEM_NAME, value: ITEM_VALUE });
const BROKEN_BODY = '{';
const MISSING_ID = 'missing';

const duration = configuredDuration();

export const options = {
  systemTags: ['status', 'method', 'name', 'scenario', 'expected_response', 'check', 'error_code'],
  summaryTrendStats: ['avg', 'min', 'max'],
  setupTimeout: '30s',
  scenarios: {
    load: {
      executor: 'externally-controlled',
      vus: VUS,
      maxVUs: VUS,
      duration,
    },
  },
};

let state = null;
let originMs = 0;
let skippedSlots = 0;
let lastReportMinute = -1;
const unexpectedLogs = {};

export function setup() {
  const bases = requiredBases();
  const ids = [];
  for (let index = 0; index < bases.length; index++) {
    const response = http.post(`${bases[index]}/items`, SEED_BODY, requestParams('POST /items', 201));
    const body = readJSON(response);
    if (response.status !== 201 || body === null || typeof body.id !== 'string' || body.id === '') {
      exec.test.abort(`setup on instance ${index + 1} failed with HTTP ${response.status}: ${snippet(response.body)}`);
    }
    ids.push(body.id);
  }
  // Two setup creates are real app traffic, but they are not paced iterations.
  console.log('setup created one item on each instance');
  return { bases, ids };
}

export default function (data) {
  if (state === null) {
    const vu = exec.vu.idInInstance - 1;
    if (!Number.isInteger(vu) || vu < 0 || vu >= VUS) {
      exec.test.abort(`unexpected VU id ${exec.vu.idInInstance}`);
    }
    state = { vu, slot: vu };
  }
  if (!data || !Array.isArray(data.bases) || !Array.isArray(data.ids) || data.ids.length !== 2) {
    exec.test.abort('setup data is missing instance ids');
  }

  for (;;) {
    const now = elapsedSeconds();
    reportPace(now);
    const decision = planIteration(state, now);
    if (decision.action === 'sleep') {
      sleep(decision.seconds);
      continue;
    }
    if (decision.action === 'skip') {
      skippedSlots += decision.skipped;
      state = { vu: state.vu, slot: decision.slot };
      continue;
    }
    break;
  }

  issue(data, state.slot);
  const advanced = advanceAfterRequest(state, elapsedSeconds());
  skippedSlots += advanced.skipped;
  state = { vu: state.vu, slot: advanced.slot };
}

function configuredDuration() {
  const raw = __ENV.LOAD_DURATION;
  if (raw === undefined || String(raw).trim() === '') return '0s';
  const value = String(raw).trim();
  if (!/^(\d+(\.\d+)?)(ms|s|m|h)$/.test(value)) {
    throw new Error('LOAD_DURATION must be a k6 duration such as 30s, or unset for the infinite Compose run');
  }
  return value;
}

function requiredBases() {
  const bases = [trimBase(__ENV.APP1_BASE_URL), trimBase(__ENV.APP2_BASE_URL)];
  if (!bases[0] || !bases[1]) {
    exec.test.abort('APP1_BASE_URL and APP2_BASE_URL are required');
  }
  if (bases[0] === bases[1]) {
    exec.test.abort('APP1_BASE_URL and APP2_BASE_URL must address different instances');
  }
  return bases;
}

function trimBase(value) {
  return String(value || '').trim().replace(/\/+$/, '');
}

function elapsedSeconds() {
  if (originMs === 0) originMs = scenarioStartMs();
  const elapsed = Date.now() - originMs;
  return elapsed > 0 ? elapsed / 1000 : 0;
}

function scenarioStartMs() {
  const start = exec.scenario.startTime;
  if (start instanceof Date) return start.getTime();
  if (typeof start === 'number' && Number.isFinite(start)) {
    return start < 1e12 ? start * 1000 : start;
  }
  const parsed = Date.parse(String(start));
  if (Number.isFinite(parsed)) return parsed;
  exec.test.abort('k6 scenario start time is unavailable');
  return Date.now();
}

function reportPace(now) {
  if (state.vu !== 0) return;
  const minute = Math.floor(now / 60);
  if (minute === lastReportMinute) return;
  lastReportMinute = minute;
  console.log(`pace t=${Math.floor(now)}s target_rps=${targetRps(now).toFixed(1)} vu0_skipped_slots=${skippedSlots}`);
}

function issue(data, slot) {
  const scheduled = scheduleAt(slot);
  const base = data.bases[scheduled.pod];
  const id = data.ids[scheduled.pod];
  switch (scheduled.op) {
    case 'post':
      expectCreated(http.post(`${base}/items`, ITEM_BODY, requestParams('POST /items', 201)));
      return;
    case 'get':
      expectStored(http.get(`${base}/items/${id}`, requestParams('GET /items/{id}', 200, false)), id);
      return;
    case 'put':
      expectUpdated(http.put(`${base}/items/${id}`, ITEM_BODY, requestParams('PUT /items/{id}', 200)), id);
      return;
    case 'get404':
      expectError(
        http.get(`${base}/items/${MISSING_ID}`, requestParams('GET /items/{id}', 404, false)),
        'get404',
        404,
        'item not found',
      );
      return;
    case 'post400':
      expectError(
        http.post(`${base}/items`, BROKEN_BODY, requestParams('POST /items', 400)),
        'post400',
        400,
        'invalid request',
      );
      return;
    case 'put400':
      expectError(
        http.put(`${base}/items/${id}`, BROKEN_BODY, requestParams('PUT /items/{id}', 400)),
        'put400',
        400,
        'invalid request',
      );
      return;
    default:
      exec.test.abort(`unknown operation ${scheduled.op}`);
  }
}

function requestParams(name, status, json = true) {
  const params = {
    tags: { name },
    timeout: '10s',
    responseType: 'text',
    responseCallback: http.expectedStatuses(status),
  };
  if (json) params.headers = { 'Content-Type': 'application/json' };
  return params;
}

function expectCreated(response) {
  const body = readJSON(response);
  const ok = check(response, {
    'post status 201': (res) => res.status === 201,
    'post body': () => body !== null && typeof body.id === 'string' && body.id !== ''
      && body.name === ITEM_NAME && body.value === ITEM_VALUE,
  });
  if (!ok) reportBad('post', response.status, response.status === 201 ? 'body' : 'status');
}

function expectStored(response, id) {
  const body = readJSON(response);
  const ok = check(response, {
    'get status 200': (res) => res.status === 200,
    'get body': () => body !== null && body.id === id && knownItem(body),
  });
  if (!ok) reportBad('get', response.status, response.status === 200 ? 'body' : 'status');
}

function expectUpdated(response, id) {
  const body = readJSON(response);
  const ok = check(response, {
    'put status 200': (res) => res.status === 200,
    'put body': () => body !== null && body.id === id && body.name === ITEM_NAME && body.value === ITEM_VALUE,
  });
  if (!ok) reportBad('put', response.status, response.status === 200 ? 'body' : 'status');
}

function expectError(response, op, status, message) {
  const body = readJSON(response);
  const ok = check(response, {
    [`${op} status ${status}`]: (res) => res.status === status,
    [`${op} error`]: () => body !== null && body.error === message,
  });
  if (!ok) reportBad(op, response.status, response.status === status ? 'body' : 'status');
}

function knownItem(body) {
  return (body.name === SEED_NAME && body.value === SEED_VALUE)
    || (body.name === ITEM_NAME && body.value === ITEM_VALUE);
}

function readJSON(response) {
  try {
    return response.json();
  } catch (error) {
    return null;
  }
}

function reportBad(op, status, reason) {
  exec.test.fail();
  const key = `${op}:${status}:${reason}`;
  const count = (unexpectedLogs[key] || 0) + 1;
  unexpectedLogs[key] = count;
  if (count <= 3 || count === 100 || count === 1000) {
    console.error(`unexpected ${op} response (${reason}) status ${status}, occurrence ${count} on this VU`);
  }
}

function snippet(body) {
  const text = String(body || '').replace(/\s+/g, ' ').trim();
  return text.length > 120 ? `${text.slice(0, 120)}...` : text;
}
