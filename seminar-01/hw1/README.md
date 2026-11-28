# HW-1 — Grafana automation tools (Python)

A small pet-project: a CRUD microservice with Prometheus golden signals, an
automated Grafana dashboard generator, and a load tool. Everything runs with
`docker compose up`.

## Run

```bash
cp .env.example .env
docker compose up
```

Within 2–3 minutes Grafana shows a live dashboard under load.

## Ports

| Service     | URL                  |
|-------------|----------------------|
| app1        | http://127.0.0.1:8090 |
| app2        | http://127.0.0.1:8091 |
| Prometheus  | http://127.0.0.1:9090 |
| Grafana     | http://127.0.0.1:3000 |

App endpoints: `POST /items`, `GET /items/{id}` (404 on unknown id), `PUT /items/{id}`,
plus `GET /health` (liveness), `GET /metrics` (Prometheus exposition), `GET /handlers`
(runtime handler list used by the dashboard generator).

## Metrics and labels

Chosen names and labels (this is the "library" part — implemented as an idiomatic
aiohttp middleware in `app/metrics.py`):

- **`http_requests_total`** — Counter, labels `method`, `handler`, `code`, `pod`, `datacenter`.
  Serves throughput (RPS via `rate(...)`) and errors (filter `code` to `4xx`/`5xx`,
  broken down by concrete code).
- **`http_request_duration_seconds`** — Histogram, labels `method`, `handler`, `pod`, `datacenter`.
  Serves latency percentiles.

Labels:
- `pod` ← `POD_ID`, `datacenter` ← `DATACENTER` (runtime env, not compile-time).
- `handler` is the route **template** (`/items/{id}`), not the actual request path;
  `method` is the HTTP method. Together they identify a handler (e.g. `POST /items`,
  `GET /items/{id}`).
- `code` is the response status (on the Counter).

`/health`, `/metrics` and `/handlers` are service endpoints and are **not** instrumented.

## Why a Histogram (not a Summary) for latency

A Summary computes quantiles **inside a single process**; its quantiles cannot be
combined across instances, so a multi-pod aggregate would be wrong. A Histogram
exposes `_bucket`, `_sum`, `_count`, so percentiles stay correct when aggregated
across pods/datacenters by summing buckets over `le` and applying
`histogram_quantile`:

```promql
histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket{...}[5m])))
```

That is exactly what the generator emits for p50/p95/p99, and `sum(rate(...))` is
used for RPS.

## Dashboard

The generator (`tools/generate_dashboard.py`) reads `GET /handlers` **at runtime**, so
adding a handler to the app shows up on the dashboard without editing the script. It
builds, per handler, panels for latency percentiles, RPS, and errors by code. The
dashboard is **non-editable**, uses the provisioned datasource `hw1-prom` (fixed uid),
and has `datacenter` and `pod` selectors (multi / All) with PromQL that aggregates
correctly across the selected values.

## Load tool

`tools/load.py` ramps to ~35 RPS total over 2 minutes and holds it. It spreads requests
across both instances and all handlers. Errors come from: `404` (a few % of requests hit
a nonexistent id) and `500` (artificial app faults via `ERROR_RATE`), so at least two
codes appear on the dashboard.

## Secrets

No secrets are committed. `.env.example` contains a demo sample value; the real `.env`
is gitignored. Grafana admin credentials are passed through environment variables.

## Project layout

```
hw1/
  compose.yaml
  .env.example
  .gitignore
  app/          Dockerfile, requirements.txt, metrics.py (library), server.py
  tools/        Dockerfile, requirements.txt, generate_dashboard.py, load.py
  prometheus/   prometheus.yml
  grafana/      provisioning/datasources/datasource.yml
```
