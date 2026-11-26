import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { readConfig } from '../src/producer/config.js';
import { runProducer } from '../src/producer/producer.js';
import type { Producer } from 'kafkajs';
const config = readConfig({ EVENT_INTERVAL_MS: '1' });

test('signal during send drains it before one disconnect, repeated signal safe', async () => {
  const signals = new EventEmitter(); const order: string[] = [];
  let release!: () => void; const blocked = new Promise<void>(r => { release = r; });
  let entered!: () => void; const started = new Promise<void>(r => { entered = r; });
  const code = runProducer(config, {
    connect: async () => {},
    send: async request => { assert.equal(request.acks, -1); assert.equal(request.messages.length, 1); order.push('send'); entered(); await blocked; order.push('ack'); return []; },
    disconnect: async () => { order.push('disconnect'); },
  }, signals);
  await started; signals.emit('SIGTERM'); signals.emit('SIGINT'); assert.deepEqual(order, ['send']);
  release(); assert.equal(await code, 0); assert.deepEqual(order, ['send', 'ack', 'disconnect']); assert.equal(signals.listenerCount('SIGTERM'), 0);
});
for (const signal of ['SIGTERM', 'SIGINT']) test(`${signal} interrupts interval without another send`, async () => {
  const signals = new EventEmitter(); let sends = 0;
  const code = await runProducer({ ...config, intervalMs: 60000 }, {
    connect: async () => {}, send: async () => { sends++; setImmediate(() => signals.emit(signal)); return []; }, disconnect: async () => {},
  }, signals);
  assert.equal(code, 0); assert.equal(sends, 1);
});
for (const failure of ['connect', 'send', 'disconnect']) test(`${failure} failure yields nonzero exit and no skipped event`, async () => {
  const signals = new EventEmitter(); let sends = 0; let disconnected = 0;
  const producer: Pick<Producer, 'connect' | 'send' | 'disconnect'> = {
    connect: async () => { if (failure === 'connect') throw Error('connect test'); },
    send: async () => { sends++; if (failure === 'send') throw Error('send test'); signals.emit('SIGTERM'); return []; },
    disconnect: async () => { disconnected++; if (failure === 'disconnect') throw Error('disconnect test'); },
  };
  assert.equal(await runProducer(config, producer, signals), 1); assert.equal(disconnected, 1); assert.equal(sends, failure === 'connect' ? 0 : 1);
});
test('empty ownership stays idle and stops cleanly', async () => {
  const signals = new EventEmitter();
  const code = runProducer(readConfig({ PRODUCER_ID: 'producer-3', USER_COUNT: '1' }), {
    connect: async () => { setImmediate(() => signals.emit('SIGTERM')); }, send: async () => { throw Error('unexpected send'); }, disconnect: async () => {},
  }, signals);
  assert.equal(await code, 0);
});
