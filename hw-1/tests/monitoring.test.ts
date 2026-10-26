import assert from "node:assert/strict";
import { test } from "node:test";
import { createApp } from "../src/app.ts";
import { productRoutes } from "../src/products.ts";

const identity = { podId: "test-pod", datacenter: "test-dc" };
const metrics = async (app: ReturnType<typeof createApp>) =>
  (await app.inject("/metrics")).body;
function total(text: string, name: string, status?: number) {
  return text
    .split("\n")
    .filter(
      (line) =>
        line.startsWith(`${name}{`) &&
        (status === undefined || line.includes(`status_code="${status}"`)),
    )
    .reduce(
      (sum, line) => sum + Number(line.slice(line.lastIndexOf(" ") + 1)),
      0,
    );
}

test("Counter increases and Histogram records request duration", async (t) => {
  const app = createApp(identity);

  t.after(() => app.close());

  const before = await metrics(app);
  await app.inject({
    method: "POST",
    url: "/items",
    payload: { name: "Клавиатура", price: 450000 },
  });
  await app.inject("/items/missing");
  const after = await metrics(app);

  assert.equal(
    total(after, "http_requests_total") - total(before, "http_requests_total"),
    2,
  );
  assert.equal(total(after, "http_request_duration_seconds_count"), 2);
  assert.ok(total(after, "http_request_duration_seconds_sum") > 0);
});

test("400, 404 and 500 responses have the correct metric statuses", async (t) => {
  const app = createApp(identity, {
    businessRoutes: async (business) => {
      business.register(productRoutes);
      business.get("/failure", async () => {
        throw new Error("test failure");
      });
    },
  });

  t.after(() => app.close());

  assert.equal(
    (await app.inject({ method: "POST", url: "/items", payload: {} }))
      .statusCode,
    400,
  );

  assert.equal((await app.inject("/items/missing")).statusCode, 404);
  assert.equal((await app.inject("/failure")).statusCode, 500);

  const text = await metrics(app);

  for (const status of [400, 404, 500]) {
    assert.equal(total(text, "http_requests_total", status), 1);
  }

  assert.equal(total(text, "http_requests_total"), 3);
});

test("/handlers returns business templates and service requests do not change metrics", async (t) => {
  const app = createApp(identity);

  t.after(() => app.close());

  await app.inject("/items/missing");
  const before = await metrics(app);
  const catalog = await app.inject("/handlers");

  assert.equal(catalog.statusCode, 200);

  assert.deepEqual(catalog.json().handlers, [
    { method: "GET", path: "/items/:id" },
    { method: "POST", path: "/items" },
    { method: "PUT", path: "/items/:id" },
  ]);

  assert.equal((await app.inject("/health")).statusCode, 200);

  assert.equal(await metrics(app), before);
});
