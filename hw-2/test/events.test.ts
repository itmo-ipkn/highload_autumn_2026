import test from 'node:test';
import assert from 'node:assert/strict';
import { readConfig } from '../src/config.js';
import { createGenerator, eventTypes, toMessage, usersFor } from '../src/events.js';

test('configuration defaults and invalid values', () => {
  const c = readConfig({});
  assert.equal(c.keyMode, 'user_id'); assert.equal(c.intervalMs, 100); assert.equal(c.userCount, 30); assert.equal(c.poisonRate, .02);
  for (const env of [{ KEY_MODE: 'user' }, { PRODUCER_ID: 'producer-4' }, { USER_COUNT: '0' }, { USER_COUNT: '1.1' }, { EVENT_INTERVAL_MS: 'NaN' }, { EVENT_INTERVAL_MS: '2147483648' }, { POISON_RATE: '' }, { POISON_RATE: '-1' }, { POISON_RATE: '1.01' }, { KAFKA_BROKERS: '' }, { KAFKA_BROKERS: 'host:99999' }, { KAFKA_TOPIC: '' }]) assert.throws(() => readConfig(env));
});
test('disjoint and complete ownership including empty sets', () => {
  for (const n of [1, 2, 9, 30, 31]) {
    const groups = [0, 1, 2].map(i => usersFor(i, n));
    const all = groups.flat(); assert.equal(all.length, n); assert.equal(new Set(all).size, n);
    for (let i = 1; i <= n; i++) assert.ok(groups[(i - 1) % 3]!.includes(`user-${i}`));
  }
  assert.equal(createGenerator([], .02)(), undefined);
});
test('event shape, UUID uniqueness, per-user seq and round robin', () => {
  const next = createGenerator(['user-1', 'user-4'], 0);
  const ids = new Set();
  for (let i = 0; i < 12; i++) {
    const e = next()!; assert.equal(e.user_id, i % 2 ? 'user-4' : 'user-1'); assert.equal(e.seq, Math.floor(i / 2) + 1);
    assert.match(e.event_id, /^[\da-f]{8}-[\da-f]{4}-4[\da-f]{3}-[89ab][\da-f]{3}-[\da-f]{12}$/);
    assert.equal(new Date(e.produced_at).toISOString(), e.produced_at); assert.ok(eventTypes.includes(e.type));
    assert.deepEqual(e.payload, {}); ids.add(e.event_id);
  }
  assert.equal(ids.size, 12);
});
test('poison probability threshold, types, seq and keys', () => {
  for (const [sample, poison] of [[0, true], [.0199, true], [.02, false], [.99, false]] as const) {
    const e = createGenerator(['user-1'], .02, () => sample)()!;
    assert.equal(e.payload.poison === true, poison);
  }
  for (const [sample, type] of [[0, 'page_view'], [.4, 'add_to_cart'], [.9, 'purchase']] as const) assert.equal(createGenerator(['u'], 0, () => sample)()!.type, type);
  const next = createGenerator(['user-1'], 1, () => .99);
  for (let seq = 1; seq <= 3; seq++) {
    const e = next()!; assert.equal(e.seq, seq); assert.equal(e.payload.poison, true);
    const keyed = toMessage(e, 'user_id'); const plain = toMessage(e, 'none');
    assert.equal(keyed.key, e.user_id); assert.equal(plain.key, null); assert.equal(keyed.value, plain.value);
    assert.deepEqual(JSON.parse(plain.value), e); assert.ok(!('partition' in plain));
  }
});
