import type { Handler } from "./handlers.js";

const datasource = { type: "prometheus", uid: "items-prometheus" };
const scope = 'pod_id=~"${pod_id:regex}",datacenter=~"${datacenter:regex}"';
const rateInterval = "$__rate_interval";

const target = (expr: string, legendFormat: string, refId = "A") => ({
  refId,
  expr,
  legendFormat,
});

const createPanel = (
  id: number,
  title: string,
  unit: string,
  targets: ReturnType<typeof target>[],
  x: number,
  y: number,
  width = 8,
) => ({
  id,
  title,
  type: "timeseries",
  datasource,
  targets,
  gridPos: { h: 8, w: width, x, y },
  fieldConfig: { defaults: { unit, min: 0 } },
  options: {
    tooltip: { mode: "multi", sort: "desc" },
    legend: { showLegend: true, displayMode: "list", placement: "bottom" },
  },
});

const createVariable = (name: string, label: string, query: string) => ({
  name,
  label,
  type: "query",
  datasource,
  query: { query, refId: "PrometheusVariableQueryEditor-VariableQuery" },
  refresh: 1,
  multi: true,
  includeAll: true,
  allValue: ".*",
  current: { text: "All", value: "$__all" },
  sort: 1,
});

export function createDashboard(handlers: Handler[]) {
  const panels = [
    createPanel(1, "Total RPS", "reqps", [
      target(`sum(rate(http_requests_total{${scope}}[${rateInterval}]))`, "Total"),
    ], 0, 0, 12),
    createPanel(2, "RPS by pod / datacenter", "reqps", [
      target(
        `sum by (pod_id, datacenter) (rate(http_requests_total{${scope}}[${rateInterval}]))`,
        "{{pod_id}} / {{datacenter}}",
      ),
    ], 12, 0, 12),
  ];

  for (const [index, handler] of handlers.entries()) {
    const filter = `method=${JSON.stringify(handler.method)},route=${JSON.stringify(handler.path)},${scope}`;
    const label = `${handler.method} ${handler.path}`;
    const id = 3 + index * 3;
    const y = 8 + index * 8;
    const latencyTargets = [
      { quantile: 0.5, legend: "p50", refId: "A" },
      { quantile: 0.95, legend: "p95", refId: "B" },
      { quantile: 0.99, legend: "p99", refId: "C" },
    ].map(({ quantile, legend, refId }) => target(
      `histogram_quantile(${quantile}, sum by (le) (rate(http_request_duration_seconds_bucket{${filter}}[${rateInterval}])))`,
      legend,
      refId,
    ));

    panels.push(
      createPanel(id, `${label} — Latency`, "s", latencyTargets, 0, y),
      createPanel(id + 1, `${label} — RPS`, "reqps", [
        target(`sum(rate(http_requests_total{${filter}}[${rateInterval}]))`, "RPS"),
      ], 8, y),
      createPanel(id + 2, `${label} — Errors`, "reqps", [
        target(`sum by (status_code) (rate(http_requests_total{${filter},status_code=~"4..|5.."}[${rateInterval}]))`, "{{status_code}}"),
      ], 16, y),
    );
  }

  return {
    uid: "items-generated-test",
    title: "Product Catalog — Generated",
    schemaVersion: 41,
    editable: false,
    refresh: "5s",
    time: { from: "now-15m", to: "now" },
    panels,
    templating: {
      list: [
        createVariable("datacenter", "Datacenter", "label_values(http_requests_total, datacenter)"),
        createVariable("pod_id", "Pod", 'label_values(http_requests_total{datacenter=~"${datacenter:regex}"}, pod_id)'),
      ],
    },
  };
}
