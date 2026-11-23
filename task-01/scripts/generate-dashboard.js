const http = require("http");
const https = require("https");
const crypto = require("crypto");

console.log("Generator started");

const GRAFANA_URL = "http://grafana:3000";
const GRAFANA_USER = "admin";
const GRAFANA_PASS = process.env.GRAFANA_ADMIN_PASSWORD;
const DASHBOARD_NAME = "task-1 Andreeva Anna";

const DASHBOARD_UID = crypto.randomBytes(8).toString("hex");

const dashboardJSON = {
  dashboard: {
    annotations: { list: [] },
    editable: true,
    fiscalYearStartMonth: 0,
    graphTooltip: 1,
    links: [],
    panels: [
      {
        gridPos: { h: 8, w: 12, x: 0, y: 0 },
        id: 1,
        title: "Latency",
        type: "timeseries",
        targets: [
          {
            expr: 'histogram_quantile(0.50, sum by (le, datacenter, pod_id) (rate(http_request_duration_seconds_bucket{datacenter=~"$datacenter", pod_id=~"$pod_id"}[5m])))',
            legendFormat: "p50",
          },
          {
            expr: 'histogram_quantile(0.95, sum by (le, datacenter, pod_id) (rate(http_request_duration_seconds_bucket{datacenter=~"$datacenter", pod_id=~"$pod_id"}[5m])))',
            legendFormat: "p95",
          },
          {
            expr: 'histogram_quantile(0.99, sum by (le, datacenter, pod_id) (rate(http_request_duration_seconds_bucket{datacenter=~"$datacenter", pod_id=~"$pod_id"}[5m])))',
            legendFormat: "p99",
          },
        ],
      },
      {
        gridPos: { h: 8, w: 12, x: 12, y: 0 },
        id: 2,
        title: "Throughput",
        type: "timeseries",
        targets: [
          {
            expr: 'rate(http_requests_total{datacenter=~"$datacenter", pod_id=~"$pod_id"}[5m])',
            legendFormat: "RPS {{datacenter}}/{{pod_id}}",
          },
        ],
      },
      {
        gridPos: { h: 8, w: 12, x: 0, y: 8 },
        id: 3,
        title: "Errors (4xx/5xx)",
        type: "timeseries",
        targets: [
          {
            expr: 'sum by (status_code) (increase(http_requests_total{datacenter=~"$datacenter", pod_id=~"$pod_id", status_code=~"4\\\\d{2}|5\\\\d{2}"}[5m]))',
            legendFormat: "Errors {{status_code}}",
          },
        ],
      },
    ],
    style: "dark",
    tags: ["highload"],
    time: { from: "now-1h", to: "now" },
    timezone: "",
    title: DASHBOARD_NAME,
    uid: DASHBOARD_UID,
    version: 1,
    templating: {
      list: [
        {
          current: {},
          hide: 0,
          includeAll: true,
          multi: true,
          name: "datacenter",
          options: [],
          query: "label_values(http_requests_total, datacenter)",
          type: "query",
          useTags: false,
        },
        {
          current: {},
          hide: 0,
          includeAll: true,
          multi: true,
          name: "pod_id",
          options: [],
          query: "label_values(http_requests_total, pod_id)",
          type: "query",
          useTags: false,
        },
      ],
    },
  },
  meta: {
    isNew: true,
    canSave: true,
    canEdit: true,
  },
  overwrite: true,
};

function waitForGrafana(retries = 15, delayMs = 3000) {
  return new Promise((resolve, reject) => {
    const tryConnect = (remaining) => {
      const parsed = new URL(GRAFANA_URL);
      const lib = parsed.protocol === "https:" ? https : http;

      const req = lib.request(
        {
          hostname: parsed.hostname,
          port: parsed.port || (parsed.protocol === "https:" ? 443 : 80),
          path: "/login",
          method: "GET",
          timeout: 5000,
        },
        (res) => {
          if (res.statusCode === 200 || res.statusCode === 401) {
            console.log(`Grafana is reachable (status ${res.statusCode})`);
            resolve();
            return;
          }
          handleRetry(remaining);
        },
      );

      req.on("error", () => handleRetry(remaining));
      req.end();

      function handleRetry(rem) {
        if (rem <= 0) {
          reject(new Error("Grafana did not become ready in time"));
          return;
        }
        console.log(`Grafana not ready yet, waiting... (${rem} retries left)`);
        setTimeout(() => tryConnect(rem - 1), delayMs);
      }
    };
    tryConnect(retries);
  });
}

function sendDashboard() {
  const parsed = new URL(`${GRAFANA_URL}/api/dashboards/db`);
  const lib = parsed.protocol === "https:" ? https : http;

  const auth = Buffer.from(`${GRAFANA_USER}:${GRAFANA_PASS}`).toString(
    "base64",
  );

  const options = {
    hostname: parsed.hostname,
    port: parsed.port || (parsed.protocol === "https:" ? 443 : 80),
    path: parsed.pathname + parsed.search,
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Basic ${auth}`,
    },
    timeout: 10000,
  };

  return new Promise((resolve, reject) => {
    const req = lib.request(options, (res) => {
      let data = "";
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => {
        try {
          const body = JSON.parse(data);
          if (body.status === "success") {
            resolve(body);
          } else {
            reject(new Error(`Grafana API error: ${JSON.stringify(body)}`));
          }
        } catch (e) {
          reject(new Error(`Invalid JSON response: ${data}`));
        }
      });
    });

    req.on("error", (err) => reject(err));
    req.write(JSON.stringify(dashboardJSON));
    req.end();
  });
}

(async () => {
  try {
    await waitForGrafana(15, 3000);
    console.log("Sending dashboard to Grafana...");
    const result = await sendDashboard();
    console.log("Dashboard created/updated successfully!", result);
  } catch (err) {
    console.error("Error creating dashboard:", err.message);
  } finally {
    console.log("Generator finished successfully");
  }
})();
