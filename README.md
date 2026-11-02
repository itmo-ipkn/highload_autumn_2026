Highload HW1 — Items Service
Java-реализация задания по мониторингу CRUD-микросервиса. В проект входят два экземпляра Items Service, Prometheus, Grafana, генератор dashboard из runtime-каталога HTTP-ручек и k6-нагрузчик.  
Ссылка на видео: https://disk.yandex.ru/i/Ql89zYphBoaZjA

1\. Технологический стек

|     |     |
| --- | --- |
| Компонент | Версия |
| Java | 21  |
| Spring Boot | 4.1.2 |
| Gradle | 9.8.2 |
| Micrometr-prometheus_registry |     |
| Prometheus | 3.15 |
| Grafana | 13.2.4 |
| Python | 3.13 |
| k6  | 1.3 |
| Docker Compose | v2  |

Запуск всего стенда
```
cp .env.example .env

docker compose up -d --build
```

Endpoints  

|     |     |
| --- | --- |
| Items Service instance 1 | localhost:8080 |
| Items Service instance 2 | localhost:8081 |
| Grafana | localhost:3000 |
| Prometheus | localhost:9090 |
  
Grafana creds: admin%admin

3\. Архитектура и взаимодействие компонентов

|     |     |     |
| --- | --- | --- |
| Откуда | Куда | Endpoint |
| Prometheus | app1:8080 | GET /metrics |
| Prometheus | app2:8081 | GET /metrics |
| Grafana | prometheus:9090 | datasource |
| Dashboard Generator | app1:8080 | GET /handlers |
| Dashboard Generator | grafana:3000 | Grafana api |
| k6  | app1:8080 | rest |

Micrometer публикует percentile histogram и SLO-buckets. В PromQL сначала применяется `rate`, затем bucket-счётчики суммируются по `le`, и только после этого `histogram_quantile` вычисляет квантиль общей выборки.

Пример p95 для `GET /items/{id}` с учётом выбранных датацентров и pod:
promql
```

histogram_quantile(

0.95,

sum by (le) (

rate(http_server_request_duration_seconds_bucket{

datacenter=~"$datacenter",

pod=~"$pod",

method="GET",

route="/items/{id}"

}\[$\__rate_interval\])

)

)

```

RPS и ошибки

RPS агрегируется суммой скоростей счётчика по выбранным pod и датацентрам:

```promql

sum(

rate(http_requests_total{

datacenter=~"$datacenter",

pod=~"$pod",

method="GET",

route="/items/{id}"

}\[$\__rate_interval\])

)

```

Ошибки отображаются отдельно по HTTP-коду ответа:

```promql

sum by (status) (

rate(http_requests_total{

datacenter=~"$datacenter",

pod=~"$pod",

method="GET",

route="/items/{id}",

status=~"4..|5.."

}\[$\__rate_interval\])

)
```promql

Нагрузчик реализован на k6 в `load/test.js`. Используются два параллельных сценария `ramping-arrival-rate`: один направляет трафик только на `app1`, второй — только на `app2`.

Promql

Prometheus

up{job="items-service"} проверить коллект экземпляров apps

sum(up{job="items-service"}) метрики по двум apps
------------------------------------------
Traffic

overall requests 
sum(http_requests_total)

rps
sum(rate(http_requests_total[1m]))

rps per instance
sum by (pod) (
  rate(http_requests_total[1m])
)

methods-rps
sum by (method) (
  rate(http_requests_total[1m])
)
------------------------------------------------

Errors

404 count
sum(http_requests_total{status="404"})

500 count
sum(http_requests_total{status="500"})


% errors in summary
100 *
sum(rate(http_requests_total{status=~"4..|5.."}[1m]))
/
sum(rate(http_requests_total[1m]))

errors per instance 
sum by (pod, status) (
  rate(http_requests_total{status=~"4..|5.."}[1m])
)

errors per second

sum by (status) (
  rate(http_requests_total{status=~"4..|5.."}[1m])
)

-------------------------------------------------------------------

Latency

avg_time_requests
sum(rate(http_server_request_duration_seconds_sum[1m]))
/
sum(rate(http_requests_total[1m]))

p50 mediana
histogram_quantile(
  0.50,
  sum by (le) (
    rate(http_server_request_duration_seconds_bucket[1m])
  )
)

p90
histogram_quantile(
  0.90,
  sum by (le) (
    rate(http_server_request_duration_seconds_bucket[1m])
  )
)
p95
histogram_quantile(
  0.95,
  sum by (le) (
    rate(http_server_request_duration_seconds_bucket[1m])
  )
)
p99
histogram_quantile(
  0.99,
  sum by (le) (
    rate(http_server_request_duration_seconds_bucket[1m])
  )
)

p95 for each endpoints

histogram_quantile(
  0.95,
  sum by (pod, le) (
    rate(http_server_request_duration_seconds_bucket[1m])
  )
)

% of succeeded requests
100 *
sum(rate(http_requests_total{status=~"2.."}[1m]))
/
sum(rate(http_requests_total[1m]))
