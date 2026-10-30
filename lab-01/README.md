# sem3-highload-lab01

Работа по созданию мониторинга нагрузки для приложения на Python (FastAPI) + метрики latency, RPS и ошибок 

## Запуск

Предварительно, не забыть создать и заполнить .env

```bash
docker compose up -d
```

Разворачиваются следующие сервисы

| Адрес                                          | Сервис                                      |
| ---------------------------------------------- | ------------------------------------------- |
| http://127.0.0.1:8080                          | app1, POD_ID=pod-1, DATACENTER=ru-1         |
| http://127.0.0.1:8082                          | app2, POD_ID=pod-2, DATACENTER=eu-1         |
| http://127.0.0.1:9090                          | Prometheus                                  |
| http://127.0.0.1:3000                          | Grafana                                     |

Также разово отрабатывает генератор дашборда и в течение минуты начинает работать k6-скрипт с нагрузкой

## Метрики

Методом `GET /metrics` отдаются метрики в формате Prometheus

| Метрика | Тип | Label | Сигнал |
| --- | --- | --- | --- |
| `http_requests_total` | Counter | `method` = HTTP method (`GET`, `POST`, ...), `route` = route template (`/items/{item_id}`), `code` = exact status (`201`, `400`, `404`, ...), `pod`, `datacenter` | Throughput: `rate(http_requests_total[1m])`; Errors: per-code slices (`code="404"`, regex `"4..|5.."`) |
| `http_request_duration_seconds` | Histogram, buckets `0.5ms..10s` | `method`, `route`, `pod`, `datacenter` | Latency percentiles: `histogram_quantile(0.95, sum(rate(http_request_duration_seconds[1m])) by (method, route, le))` |

Для отображения latency выбрана Histogram, т.к оно отдаёт кумулятивные счётчики по нескольким экземплярам сразу, в отличие от других видов отображения

RPS — сумма скоростей выбранных экземпляров, не среднее. Перцентили считаются по сумме бакетов, а не усредняются между подами. `rate` берётся до суммирования, чтобы перезапуск пода не ломал расчёт.

## Ручки

| Handler | Значение |
| --- | --- |
| `POST /items` | отправляет запись в формате JSON; id генерируется по принципу sequence (`1`, `2`, ...); returns `201` + `{"id": ..., "value": ...}` |
| `GET /items/{id}` | считаывает запись с id; reuturn `404` когда id не найден |
| `PUT /items/{id}` | обновляет запись; `404` когда id не найден |
| `GET /health` | `200` если приложение доступно, `503` если недоступен |
| `GET /metrics` | Отдает метрики в формате Prometheus |
| `GET /handlers` | `200` + `{"handlers": [{"method": ..., "path": ...}, ...]}` — доступные у приложения handlers |

## Версии

* `python:3.14-slim`
* `fastapi==0.162.0`, `uvicorn==0.54.0`, `prometheus_client==0.26.0`

## Layout

```
app/main.py         -- код приложения (vault + handlers + metrics middleware)
requirements.txt    -- библиотеки (docker image + .venv)
Dockerfile
docker-compose.yaml -- build + healthcheck
.venv/              -- local dev environment (gitignored)
```
