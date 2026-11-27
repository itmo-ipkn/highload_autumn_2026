# Практика 1: userver, Prometheus и Grafana

CRUD-сервис на C++ с хранилищем в памяти и метриками запросов.
В Compose работают две реплики одного образа: `pod-1/dc-1` и `pod-2/dc-2`.
Prometheus собирает метрики каждые 5 секунд, Grafana показывает их на автоматически созданном дашборде.

## Запуск

Нужен Docker Compose или Podman с Compose. На macOS с Podman сначала запустите `podman machine start`.
Команды выполняются из корня проекта:

```bash
cp .env.example .env
docker compose up
```

| Сервис | Адрес |
| --- | --- |
| Первая реплика | http://localhost:18081 |
| Вторая реплика | http://localhost:18082 |
| Prometheus | http://localhost:9090 |
| Дашборд Grafana | http://localhost:3000/d/practice1-handlers |

Порты и остальные параметры задаются в `.env`. Внутри сети Compose приложения слушают `8080`.

## Пароль Grafana

Логин по умолчанию — `admin`. При первом запуске сервис `credentials` создаёт случайный пароль
и сохраняет его в локальном томе `grafana-credentials`. При перезапусках пароль остаётся прежним.
Получить его можно так:

```bash
docker compose run --rm --no-deps -T --entrypoint cat credentials /secrets/grafana_admin_password
```

## API

| Запрос | Результат |
| --- | --- |
| `POST /items` | Создание записи, `201` |
| `GET /items/{id}` | Чтение записи, `200` или `404` |
| `PUT /items/{id}` | Замена данных записи, `200` или `404` |
| `GET /health` | `200`, тело `OK`, если сервис готов |
| `GET /metrics` | Метрики в формате Prometheus |
| `GET /handlers` | Список операций для генератора дашборда |

В `POST` и `PUT` передаётся JSON-объект; некорректное тело возвращает `400`.
Ответ содержит `id` и `data`. Хранилища реплик независимы: запись нужно читать и обновлять на той же реплике.

```bash
curl http://localhost:18081/health
curl -i -X POST http://localhost:18081/items \
  -H 'Content-Type: application/json' -d '{"name":"example"}'
```

## Метрики

Метрики собираются через prometheus-cpp. Обёртка `MonitoredHandler` измеряет длительность
и учитывает итоговый статус ответа, в том числе после обработки исключений.
Метрики собираются для трёх бизнес-операций и `/handlers`

| Метрика | Тип и назначение |
| --- | --- |
| `http_requests_total` | Counter: число запросов по операциям и кодам ответа |
| `http_request_duration_seconds` | Histogram: длительность в секундах; экспортирует `_bucket`, `_count` и `_sum` |

Лейблы: `pod_id` из `POD_ID`, `datacenter` из `DATACENTER`, `method` и `route`.

Гистограмма выбрана, потому что бакеты разных реплик можно складывать и затем считать общий перцентиль.
Пример p95 для чтения записей по обеим репликам:

```promql
histogram_quantile(0.95, sum by (le) (
  rate(http_request_duration_seconds_bucket{job="my_service",method="GET",route="/items/{id}"}[1m])
))
```

RPS считается через `sum(rate(http_requests_total{job="my_service"}[1m]))`, ошибки — с фильтром
`status=~"4..|5.."` и группировкой `sum by (status)`. Так коды ошибок видны отдельно.

## Дашборд и нагрузка

После готовности приложений и графаны генератор читает `/handlers` и создаёт по три панели
на операцию: p50/p95/p99, RPS и ошибки. Сейчас это 12 панелей. Редактирование дашборда отключено.

После успешной генерации запускается `load`: за 120 секунд нагрузка растёт с 2 до 30 RPS
суммарно на обе реплики и продолжается до остановки. Примерно 2% запросов дают `404` и ещё 2% — `500`

Настройки в `.env`: `LOAD_RPS=30`, `LOAD_RAMP_SECONDS=120`, `LOAD_ERROR_PERCENT=4`, `LOAD_WORKERS=16`.
Проверить работу стенда:

```bash
docker compose ps
docker compose logs dashboard-generator
docker compose logs -f load
```

## Остановка

```bash
docker compose stop load  # остановить только нагрузку; возобновить: docker compose up -d load
docker compose stop       # остановить весь стенд
docker compose down       # удалить контейнеры, сохранив тома
```

Записи приложения теряются при перезапуске. База Grafana, пароль и история Prometheus остаются в томах.
