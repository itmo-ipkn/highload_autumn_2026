import type { Handler } from "./handlers.js";

type PanelMetric = "RPS" | "Errors" | "p50" | "p95" | "p99";

const createQuery = (handler: Handler, metric: PanelMetric): string => {
  const filter = `method="${handler.method}",route="${handler.path}"`;

  if (metric === "RPS") {
    return `sum(rate(http_requests_total{${filter}}[1m]))`;
  }

  if (metric === "Errors") {
    return `sum(rate(http_requests_total{${filter},status_code=~"5.."}[1m]))`;
  }

  const quantiles = {
    p50: 0.5,
    p95: 0.95,
    p99: 0.99,
  };

  return `histogram_quantile(${quantiles[metric]}, sum by (le) (rate(http_request_duration_seconds_bucket{${filter}}[1m])))`;
};

const createPanel = (handler: Handler, metric: PanelMetric, index: number) => {
  const expr = createQuery(handler, metric);

  return {
    id: index + 1,
    title: `${handler.method} ${handler.path} — ${metric}`,
    type: "timeseries",

    datasource: {
      type: "prometheus",
      uid: "items-prometheus",
    },

    targets: [
      {
        refId: "A",
        expr,
      },
    ],

    gridPos: {
      h: 8,
      w: 12,
      x: (index % 2) * 12,
      y: Math.floor(index / 2) * 8,
    },

    fieldConfig: {
      defaults: {
        unit: metric.startsWith("p") ? "s" : "short",
      },
    },
  };
};

export function createDashboard(handlers: Handler[]) {
  const metrics: PanelMetric[] = ["RPS", "Errors", "p50", "p95", "p99"];

  const panels = handlers.flatMap((handler, handlerIndex) =>
    metrics.map((metric, metricIndex) =>
      createPanel(handler, metric, handlerIndex * metrics.length + metricIndex),
    ),
  );

  return {
    uid: "items-generated-test",
    title: "Product Catalog — Generated",
    schemaVersion: 41,
    panels,
  };
}
