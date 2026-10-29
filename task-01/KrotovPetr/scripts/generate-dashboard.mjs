const datasource = { type: "prometheus", uid: "prometheus-main" };
const dashboardUid = "items-api-handlers";
const variableFilters = 'datacenter=~"${datacenter:regex}",pod_id=~"${pod_id:regex}"';

function requiredEnvironment(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} must be set`);
  }
  return value;
}

function labelMatchers(handler) {
  return [
    `method=${JSON.stringify(handler.method)}`,
    `route=${JSON.stringify(handler.path)}`,
    variableFilters,
  ].join(",");
}

function variable(name, label) {
  return {
    name,
    label,
    type: "query",
    datasource,
    query: `label_values(http_requests_total, ${name})`,
    refresh: 1,
    sort: 1,
    multi: true,
    includeAll: true,
    allValue: ".*",
    current: { selected: true, text: "All", value: "$__all" },
    options: [],
  };
}

function panel(id, title, x, y, unit, queries) {
  return {
    id,
    title,
    type: "timeseries",
    datasource,
    gridPos: { x, y, w: 8, h: 8 },
    fieldConfig: {
      defaults: { unit },
      overrides: [],
    },
    options: {
      legend: { displayMode: "list", placement: "bottom" },
      tooltip: { mode: "multi" },
    },
    targets: queries.map(({ expr, legendFormat }, index) => ({
      datasource,
      expr,
      legendFormat,
      refId: String.fromCharCode(65 + index),
    })),
  };
}

function panelsForHandler(handler, row) {
  const matchers = labelMatchers(handler);
  const durationBuckets =
    `http_request_duration_seconds_bucket{${matchers}}[$__rate_interval]`;
  const requests = `http_requests_total{${matchers}}`;
  const title = `${handler.method} ${handler.path}`;
  const y = row * 8;
  const id = row * 3 + 1;

  return [
    panel(
      id,
      `${title} · latency`,
      0,
      y,
      "s",
      [
        { quantile: 0.5, legendFormat: "p50" },
        { quantile: 0.95, legendFormat: "p95" },
        { quantile: 0.99, legendFormat: "p99" },
      ].map(({ quantile, legendFormat }) => ({
        expr: `histogram_quantile(${quantile}, sum by (le) (rate(${durationBuckets})))`,
        legendFormat,
      })),
    ),
    panel(id + 1, `${title} · RPS`, 8, y, "reqps", [
      {
        expr: `sum(rate(${requests}[$__rate_interval]))`,
        legendFormat: "RPS",
      },
    ]),
    panel(id + 2, `${title} · errors by code (5m)`, 16, y, "short", [
      {
        expr: `sum by (status_code) (increase(http_requests_total{${matchers},status_code=~"[45].."}[5m]))`,
        legendFormat: "{{status_code}}",
      },
    ]),
  ];
}

function buildDashboard(handlers) {
  return {
    id: null,
    uid: dashboardUid,
    title: "Items API · REST handlers",
    tags: ["ddia", "task-01"],
    schemaVersion: 39,
    version: 0,
    editable: false,
    timezone: "browser",
    refresh: "5s",
    time: { from: "now-15m", to: "now" },
    templating: {
      list: [variable("datacenter", "Datacenter"), variable("pod_id", "Pod")],
    },
    annotations: { list: [] },
    panels: handlers.flatMap(panelsForHandler),
  };
}

async function requestJson(url, options = {}) {
  const response = await fetch(url, {
    ...options,
    signal: AbortSignal.timeout(15_000),
  });
  if (!response.ok) {
    throw new Error(`${url}: HTTP ${response.status}: ${await response.text()}`);
  }
  return response.json();
}

function validateHandlers(value) {
  if (
    !Array.isArray(value) ||
    value.length === 0 ||
    !value.every(
      (handler) =>
        typeof handler === "object" &&
        handler !== null &&
        typeof handler.method === "string" &&
        typeof handler.path === "string" &&
        handler.path.startsWith("/"),
    )
  ) {
    throw new Error("/handlers returned no valid working routes");
  }
  return value;
}

async function main() {
  const appUrl = requiredEnvironment("APP_URL");
  const grafanaUrl = requiredEnvironment("GRAFANA_URL");
  const username = requiredEnvironment("GRAFANA_ADMIN_USER");
  const password = requiredEnvironment("GRAFANA_ADMIN_PASSWORD");
  const authorization = `Basic ${Buffer.from(`${username}:${password}`).toString("base64")}`;

  const { handlers: rawHandlers } = await requestJson(`${appUrl}/handlers`);
  const handlers = validateHandlers(rawHandlers);
  const dashboard = buildDashboard(handlers);
  const headers = {
    Authorization: authorization,
    "Content-Type": "application/json",
  };

  await requestJson(`${grafanaUrl}/api/dashboards/db`, {
    method: "POST",
    headers,
    body: JSON.stringify({ dashboard, overwrite: true }),
  });
  await requestJson(`${grafanaUrl}/api/org/preferences`, {
    method: "PATCH",
    headers,
    body: JSON.stringify({ homeDashboardUID: dashboardUid }),
  });
  console.log(`Generated dashboard ${dashboardUid} from ${handlers.length} handlers`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
