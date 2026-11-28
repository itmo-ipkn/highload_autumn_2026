# Task 1: Items API

This directory contains the in-memory microservice, two application instances, Prometheus scraping, a generated Grafana dashboard, and a background load generator for Task 1.

## Run

```sh
cp .env.example .env
docker compose up --build
```

The first API instance listens on `http://localhost:8080`, the second on `http://localhost:8081`, Prometheus on `http://localhost:9090`, and Grafana on `http://localhost:3000`. Change the corresponding port in `.env` if needed. Both application containers use the same image. Each has its own in-memory storage, which disappears when that container restarts.

Grafana uses the admin credentials from `.env`. The value in `.env.example` is a public, local-only example; replace it in `.env` if you expose Grafana outside localhost. Existing `.env` files made before Grafana was added need a `GRAFANA_ADMIN_PASSWORD` entry.

## Endpoints

| Method | Path | Result |
| --- | --- | --- |
| `POST` | `/items` | Create an item from `{"name":"example"}`; returns 201 |
| `GET` | `/items/{id}` | Return an item; returns 404 if it does not exist |
| `PUT` | `/items/{id}` | Replace an item's name from `{"name":"updated"}`; returns 404 if it does not exist |
| `GET` | `/health` | Return 200 while the service is ready |
| `GET` | `/metrics` | Return Prometheus metrics |
| `GET` | `/handlers` | List the application's working REST routes |

An invalid item ID or missing/empty `name` returns 400.

## Metrics

`POD_ID` and `DATACENTER` are read from the environment. They appear on both application metrics as `pod_id` and `datacenter` labels. The second instance receives `POD_ID_2` and `DATACENTER_2` from Compose. The defaults are `pod-1`/`dc-a` and `pod-2`/`dc-b`.

| Metric | Type | Labels | Use |
| --- | --- | --- | --- |
| `http_requests_total` | Counter | `method`, `route`, `status_code`, `pod_id`, `datacenter` | Request rate and 4xx/5xx counts by exact code |
| `http_request_duration_seconds` | Histogram | `method`, `route`, `pod_id`, `datacenter`; generated bucket label `le` | p50, p95, and p99 latency |

`route` contains a template such as `/items/{id}`, never the ID from a request. `/health` and `/metrics` are excluded. The histogram uses buckets at 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, and 5 seconds, plus the automatic `+Inf` bucket. These boundaries give finer detail for quick in-memory requests while still covering slower requests.

A Histogram exposes cumulative buckets that can be summed across instances while retaining the `le` boundary, then passed to `histogram_quantile`. A Summary calculates quantiles in each instance; those quantiles cannot be combined into a correct overall percentile.

Prometheus scrapes `app:8080/metrics` and `app-b:8080/metrics` every five seconds. Check target status at `http://localhost:9090/targets`. The `/handlers` response is built from the same route registrations as the API, so registering another working route through `registerHandler` adds it to the list without editing `/handlers`.

## Grafana dashboard

The Prometheus data source is provisioned with the fixed UID `prometheus-main`. After the application and Grafana health checks pass, `dashboard-generator` reads `/handlers` and creates or updates the dashboard with UID `items-api-handlers`. It also makes this dashboard the Grafana organization home page. The dashboard is marked non-editable and has one row of latency, RPS, and errors-by-code panels per working route. Its Datacenter and Pod selectors allow multiple values or All.

The generator obtains the route list at runtime. Adding a route through `registerHandler` and restarting the stack adds its panels without changing the generator. The dashboard uses `sum(rate(...))` for RPS and sums histogram bucket rates by `le` before calculating p50, p95, and p99. Errors show the rolling five-minute count for each 4xx or 5xx code. The generator authenticates to Grafana using the admin credentials passed as environment variables.

## Background load

After `dashboard-generator` exits successfully, `load-generator` starts automatically. It creates one item in each application instance, then sends POST, GET, and PUT requests to both instances. The rate rises linearly over two minutes to 30 requests per second in total and stays there until the stack stops. Change `LOAD_TARGET_RPS` and `LOAD_RAMP_SECONDS` in `.env` to adjust it.

About 3% of requests ask for a missing item and return 404. Another 2% send `X-Demo-Failure: 1` and return 500. This header only triggers a simulated error on the three working routes when `DEMO_FAILURES=1`, which Compose sets for the demo; `/health` and `/metrics` are unaffected. The 404 and 500 proportions can be changed with `LOAD_404_RATE` and `LOAD_500_RATE`. View response counts with `docker compose logs -f load-generator`.
