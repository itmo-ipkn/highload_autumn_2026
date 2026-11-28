const assert = require("node:assert/strict");
const { once } = require("node:events");
const test = require("node:test");
const { createApp } = require("../dist/app.js");

test("records templated routes, response codes, and instance labels", async (context) => {
  const server = createApp({ podId: "pod-test", datacenter: "dc-test" }, true).listen(
    0,
    "127.0.0.1",
  );
  await once(server, "listening");
  context.after(() => new Promise((resolve) => server.close(resolve)));

  const base = `http://127.0.0.1:${server.address().port}`;
  const health = await fetch(`${base}/health`);
  assert.equal(health.status, 200);

  const handlers = await fetch(`${base}/handlers`);
  assert.equal(handlers.status, 200);
  assert.deepEqual(await handlers.json(), {
    handlers: [
      { method: "POST", path: "/items" },
      { method: "GET", path: "/items/{id}" },
      { method: "PUT", path: "/items/{id}" },
    ],
  });

  const created = await fetch(`${base}/items`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ name: "first" }),
  });
  assert.equal(created.status, 201);
  const { id } = await created.json();

  const malformed = await fetch(`${base}/items`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: '{"name":',
  });
  assert.equal(malformed.status, 400);

  assert.equal((await fetch(`${base}/items/${id}`)).status, 200);
  assert.equal((await fetch(`${base}/items/999`)).status, 404);
  assert.equal(
    (await fetch(`${base}/items/${id}`, {
      headers: { "x-demo-failure": "1" },
    })).status,
    500,
  );
  assert.equal(
    (
      await fetch(`${base}/items/${id}`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: "updated" }),
      })
    ).status,
    200,
  );

  const response = await fetch(`${base}/metrics`);
  assert.equal(response.status, 200);
  assert.match(response.headers.get("content-type"), /^text\/plain/);
  const metrics = await response.text();
  const lines = metrics.split("\n");

  function hasMetric(prefix, labels, value) {
    return lines.some(
      (line) =>
        line.startsWith(`${prefix}{`) &&
        labels.every(([name, labelValue]) =>
          line.includes(`${name}="${labelValue}"`),
        ) &&
        line.endsWith(`} ${value}`),
    );
  }

  const instance = [
    ["pod_id", "pod-test"],
    ["datacenter", "dc-test"],
  ];
  assert.ok(
    hasMetric("http_requests_total", [
      ["method", "POST"],
      ["route", "/items"],
      ["status_code", "201"],
      ...instance,
    ], 1),
  );
  assert.ok(
    hasMetric("http_requests_total", [
      ["method", "GET"],
      ["route", "/handlers"],
      ["status_code", "200"],
      ...instance,
    ], 1),
  );
  assert.ok(
    hasMetric("http_requests_total", [
      ["method", "GET"],
      ["route", "/items/{id}"],
      ["status_code", "404"],
      ...instance,
    ], 1),
  );
  assert.ok(
    hasMetric("http_requests_total", [
      ["method", "GET"],
      ["route", "/items/{id}"],
      ["status_code", "500"],
      ...instance,
    ], 1),
  );
  assert.ok(
    hasMetric("http_requests_total", [
      ["method", "POST"],
      ["route", "/items"],
      ["status_code", "400"],
      ...instance,
    ], 1),
  );
  assert.ok(
    hasMetric("http_request_duration_seconds_count", [
      ["method", "GET"],
      ["route", "/items/{id}"],
      ...instance,
    ], 3),
  );
  assert.ok(!metrics.includes("/items/999"));
  assert.ok(!metrics.includes('route="/health"'));
  assert.ok(!metrics.includes('route="/metrics"'));
});
