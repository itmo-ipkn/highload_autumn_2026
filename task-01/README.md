# TASK 1. Grafana

## How to run

Copy .env and replace default values.
```
cp .env.example .env
```

Start docker containers.
```
docker compose up -d
```

If you see "No data" (on the charts) and "Datasource `prometheus-task1` was not found" (in the "Pod" UI filter), wait about 15 seconds and reload the page.

## How to open

### Grafana

Grafana dashboard: http://127.0.0.1:3000/dashboards

### Fast API

API documentation: http://127.0.0.1:8000/docs

### Prometheus

We use the same metrics names (`requests_total`, `errors_total`, `request_duration_seconds`) as the official "prometheus_client" library:
https://prometheus.github.io/client_python/instrumenting/counter/
https://prometheus.github.io/client_python/instrumenting/histogram/

Prometheus UI:
- http://127.0.0.1:9090/query?g0.expr=requests_total
- http://127.0.0.1:9090/query?g0.expr=errors_total
- http://127.0.0.1:9090/query?g0.expr=request_duration_seconds_bucket

We use five labels:
- `method` and `endpoint` to create separate rows per handler.
- `status_code` to breakdown errors or keep successful requests.
- `datacenter` and `pod_id` to create dashboard variables (UI filters).

## Notes

### Histogram

To correctly aggregate latency across all Datacenters and Pod IDs, we use the Prometheus metric type `Histogram`.
The Prometheus query function `histogram_quantile` displays the number of requests per second for a target percentile.

The "prometheus_client" library by default uses 15 duration buckets (`le`) measured in seconds (https://prometheus.github.io/client_python/instrumenting/histogram/):
```
.005, .01, .025, .05, .075, .1, .25, .5, .75, 1.0, 2.5, 5.0, 7.5, 10.0, +Inf
```

Histogram has a counter for each combination of our `prometheus_labels` values and each duration bucket. For example: `"GET" "/items" "200" "dc-a" pod_id="1" le="10.0" counter=1`.

For every request we run:
```
prometheus_latency.labels(**prometheus_labels).observe(duration)
```

It "+=1" all Histogram's counters where:
- a duration bucket >= our request duration
- and a counter has the same values in all `prometheus_labels`

Prometheus scrape (via `/metrics`) a snapshot every 5 seconds and saves it to the volume "prometheus_data". The Grafana dashboard queries data from the "prometheus-task1" datasource.

Example Grafana query for p99:
```
"histogram_quantile(0.99, sum by (le) (rate(request_duration_seconds_bucket{{endpoint='{endpoint}', method='{method}', datacenter=~'$datacenter', pod_id=~'$pod_id', status_code=~'2..'}}[1m])))"
```

- `request_duration_seconds_bucket` - returns the cumulative number of requests per combination at the scrape time.
- `rate` - converts cumulative number of requests to (new) requests per second. For example, (20 - 10) / 5 seconds = 2 request per second. The number is still at the combination level.
- `sum by (le)` - groups combinations by duration buckets (the `prometheus_labels` is ignored) and sums the number of requests per duration bucket (per second).
- `histogram_quantile(0.99, ...` - looks at the groups from the previous step and estimates the maximum duration for the 1% of the most slowest requests.
  For example:
  - The total number of requests (the `+Inf` bucket) is 100.
  - The `1.0` (1 second) bucket contains 90 of requests (latency p90 = 1 second).
  - The `2.0` (2 seconds) bucket contains 10 of requests.
  - `histogram_quantile` needs to estimate latency for p99 (because we don't have a separate 1% of requests in this particular data).

We use `sum by (le)` instead of `sum by (le, datacenter, pod_id)` because we want to calculate the metrics across all instances. To provide the ability to filter data by Datacenter and Pod ID we use `datacenter` and `pod_id` variables.
For the "Latency (2xx)" chart we use the filter `status_code=~'2..'` to keep here only successful requests (because 4xx and 5xx are quick).
