const express = require('express');
const client = require('prom-client');

const app = express();
app.use(express.json());

const register = client.register;

// Выносим лейблы на уровень модуля — читаем один раз при старте сервиса
const datacenter = process.env.DATACENTER || 'dc-unknown';
const podId = process.env.POD_ID || 'pod-unknown';

const httpRequestsTotal = new client.Counter({
  name: 'http_requests_total',
  help: 'Total HTTP requests',
  labelNames: ['method', 'endpoint', 'status_code', 'datacenter', 'pod_id'],
});

const httpRequestDurationSeconds = new client.Histogram({
  name: 'http_request_duration_seconds',
  help: 'Duration of HTTP requests in seconds',
  labelNames: ['method', 'endpoint', 'status_code', 'datacenter', 'pod_id'],
  buckets: client.linearBuckets(0.01, 0.02, 10),
});

app.use((req, res, next) => {
  const start = Date.now();

  // Нормализуем endpoint: оставляем только первый сегмент пути (например, /items вместо /items/123)
  // Это помогает агрегировать метрики по ручкам, а не по каждому ID отдельно
  const pathParts = req.path.split('/').filter(Boolean); // убираем пустые части
  const endpoint = pathParts.length > 0 ? `/${pathParts[0]}` : '/';

  res.on('finish', () => {
    const durationSec = (Date.now() - start) / 1000;
    const statusCode = String(res.statusCode);

    httpRequestsTotal.labels(
      req.method,
      endpoint,
      statusCode,
      datacenter,
      podId
    ).inc();

    httpRequestDurationSeconds.labels(
      req.method,
      endpoint,
      statusCode,
      datacenter,
      podId
    ).observe(durationSec);
  });

  next();
});

app.get('/handlers', (req, res) => {
  const handlers = [
    { path: '/health', method: 'GET', description: 'Health check' },
    { path: '/handlers', method: 'GET', description: 'List of endpoints' },
    { path: '/items', method: 'POST', description: 'Create item' },
    { path: '/items/:id', method: 'GET', description: 'Get item by ID' },
    { path: '/items/:id', method: 'PUT', description: 'Update item by ID' },
  ];
  res.json({ handlers });
});

app.get('/health', (req, res) => res.status(200).send('ok'));

const store = new Map();
let nextId = 1;

app.get('/items', (req, res) => {
  const { forceError } = req.query;
  if (forceError === '500') {
    return res.status(500).json({ error: 'Forced internal error for load testing' });
  }

  const items = Array.from(store.values());
  res.json(items);
});

app.post('/items', (req, res) => {
  const { forceError } = req.query;
  if (forceError === '500') {
    return res.status(500).json({ error: 'Forced internal error for load testing' });
  }

  const { name, description } = req.body;
  if (!name) {
    return res.status(400).json({ error: 'Name is required' });
  }
  const id = nextId++;
  const item = { id, name, description };
  store.set(id, item);
  res.status(201).json(item);
});

app.get('/items/:id', (req, res) => {
  const { forceError } = req.query;
  if (forceError === '500') {
    return res.status(500).json({ error: 'Forced internal error for load testing' });
  }

  const id = Number(req.params.id);
  const item = store.get(id);
  if (!item) {
    return res.status(404).json({ error: 'Not found' });
  }
  res.json(item);
});

app.put('/items/:id', (req, res) => {
  const { forceError } = req.query;
  if (forceError === '500') {
    return res.status(500).json({ error: 'Forced internal error for load testing' });
  }

  const id = Number(req.params.id);
  const item = store.get(id);
  if (!item) {
    return res.status(404).json({ error: 'Not found' });
  }

  const { name, description } = req.body;
  if (name !== undefined) item.name = name;
  if (description !== undefined) item.description = description;

  store.set(id, item);
  res.json(item);
});

app.get('/metrics', async (req, res) => {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`Server started on port ${PORT}, DATACENTER=${datacenter}, POD_ID=${podId}`);
});
