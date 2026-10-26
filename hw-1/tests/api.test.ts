import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createApp } from '../src/app.ts';
import type { Product } from '../src/store.ts';

test('health and full product lifecycle', async (t) => {
  const app = createApp();
  t.after(() => app.close());
  assert.equal((await app.inject('/health')).statusCode, 200);
  const created = await app.inject({ method: 'POST', url: '/items', payload: { name: '  Клавиатура  ', price: 450000 } });
  assert.equal(created.statusCode, 201);
  const product = created.json<Product>();
  assert.match(product.id, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  assert.deepEqual(product, { id: product.id, name: 'Клавиатура', price: 450000 });
  assert.deepEqual((await app.inject(`/items/${product.id}`)).json(), product);
  const replacement = { name: 'Мышь', price: 0 };
  const updated = await app.inject({ method: 'PUT', url: `/items/${product.id}`, payload: replacement });
  assert.equal(updated.statusCode, 200);
  assert.deepEqual(updated.json(), { id: product.id, ...replacement });
  assert.deepEqual((await app.inject(`/items/${product.id}`)).json(), updated.json());
  assert.equal((await app.inject('/items/missing')).statusCode, 404);
  assert.equal((await app.inject({ method: 'PUT', url: '/items/missing', payload: replacement })).statusCode, 404);
  assert.equal((await app.inject('/items/missing')).statusCode, 404);
  const second = await app.inject({ method: 'POST', url: '/items', payload: replacement });
  assert.notEqual(second.json<Product>().id, product.id);
});

test('invalid bodies return 400 and never change existing products', async (t) => {
  const app = createApp();
  t.after(() => app.close());
  const original = (await app.inject({ method: 'POST', url: '/items', payload: { name: 'Original', price: 10 } })).json<Product>();
  const invalid = [
    {}, { name: 'Valid' }, { price: 1 }, { name: '', price: 1 }, { name: '   ', price: 1 },
    { name: 'a'.repeat(201), price: 1 }, { name: 123, price: 1 },
    ...[-1, 1.5, '10', null, Number.MAX_SAFE_INTEGER + 1].map(price => ({ name: 'Valid', price })),
    { name: 'Valid', price: 1, id: 'client-id' }, { name: 'Valid', price: 1, extra: true }, [],
  ];
  for (const payload of invalid) {
    for (const method of ['POST', 'PUT'] as const) {
      const response = await app.inject({ method, url: method === 'POST' ? '/items' : `/items/${original.id}`, payload });
      assert.equal(response.statusCode, 400, `${method} ${JSON.stringify(payload)}`);
      assert.equal(typeof response.json().message, 'string');
    }
    assert.deepEqual((await app.inject(`/items/${original.id}`)).json(), original);
  }
  for (const method of ['POST', 'PUT'] as const) {
    const response = await app.inject({ method, url: method === 'POST' ? '/items' : `/items/${original.id}`, headers: { 'content-type': 'application/json' }, payload: '{' });
    assert.equal(response.statusCode, 400);
  }
  assert.deepEqual((await app.inject(`/items/${original.id}`)).json(), original);
});

test('valid boundary values and normalization', async (t) => {
  const app = createApp();
  t.after(() => app.close());
  for (const name of ['x', 'a'.repeat(200), '😀'.repeat(200)]) {
    const response = await app.inject({ method: 'POST', url: '/items', payload: { name: ` ${name} `, price: Number.MAX_SAFE_INTEGER } });
    assert.equal(response.statusCode, 201);
    assert.equal(response.json().name, name);
  }
});

test('stores are isolated and a new application starts empty', async () => {
  const first = createApp();
  const second = createApp();
  try {
    const product = (await first.inject({ method: 'POST', url: '/items', payload: { name: 'Item', price: 1 } })).json<Product>();
    assert.equal((await second.inject(`/items/${product.id}`)).statusCode, 404);
    await first.close();
    const restarted = createApp();
    try {
      assert.equal((await restarted.inject(`/items/${product.id}`)).statusCode, 404);
    } finally { await restarted.close(); }
  } finally { await first.close(); await second.close(); }
});
