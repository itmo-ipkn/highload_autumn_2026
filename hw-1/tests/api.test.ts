import assert from "node:assert/strict";
import { test } from "node:test";
import { createApp } from "../src/app.ts";
import type { Product } from "../src/store.ts";

test("creates, reads and updates a product; missing product returns 404", async (t) => {
  const app = createApp({ podId: "test-pod", datacenter: "test-dc" });

  t.after(() => app.close());

  const created = await app.inject({
    method: "POST",
    url: "/items",
    payload: { name: "  Клавиатура  ", price: 450000 },
  });

  assert.equal(created.statusCode, 201);

  const product = created.json<Product>();

  assert.deepEqual(product, {
    id: product.id,
    name: "Клавиатура",
    price: 450000,
  });
  assert.deepEqual((await app.inject(`/items/${product.id}`)).json(), product);

  const replacement = { name: "Мышь", price: 0 };
  const updated = await app.inject({
    method: "PUT",
    url: `/items/${product.id}`,
    payload: replacement,
  });

  assert.equal(updated.statusCode, 200);
  assert.deepEqual(updated.json(), { id: product.id, ...replacement });
  assert.deepEqual(
    (await app.inject(`/items/${product.id}`)).json(),
    updated.json(),
  );
  assert.equal((await app.inject("/items/missing")).statusCode, 404);
});
