// src/metrics.js
const client = require('prom-client');

// Глобальные лейблы, известные на старте (POD_ID и DATACENTER из env)
const globalLabels = {
  pod_id: process.env.POD_ID || 'unknown',
  datacenter: process.env.DATACENTER || 'unknown'
};

// Гистограмма для latency (нужна для перцентилей 50/95/99)
const requestLatencyHistogram = new client.Histogram({
  name: 'http_request_duration_seconds',
  help: 'Latency of HTTP requests in seconds',
  buckets: client.exponentialBuckets(0.005, 2, 16), // экспоненциальные бакеты: от 5 мс до ~320 сек
  labelNames: ['method', 'route', 'status_code'],
  labelValues: globalLabels
});

// Counter для throughput (RPS)
const requestTotalCounter = new client.Counter({
  name: 'http_requests_total',
  help: 'Total number of HTTP requests',
  labelNames: ['method', 'route', 'status_code'],
  labelValues: globalLabels
});

// Counter для ошибок (4xx, 5xx) с разбивкой по коду
const httpErrorsCounter = new client.Counter({
  name: 'http_errors_total',
  help: 'Total number of HTTP errors (4xx and 5xx)',
  labelNames: ['method', 'route', 'status_code'],
  labelValues: globalLabels
});

module.exports = {
  requestLatencyHistogram,
  requestTotalCounter,
  httpErrorsCounter,
  metricsEndpoint: async (req, res) => {
    const metrics = await client.register.metrics();
    res.set('Content-Type', client.register.contentType);
    res.send(metrics);
  }
};
