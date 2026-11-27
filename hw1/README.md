# My Service: Go, Prometheus, Grafana и wrk2

## Summary

- Go HTTP-сервис с in-memory `map` и потокобезопасным доступом.
- Три бизнес-ручки: `POST /items`, `GET /items/{id}`, `PUT /items/{id}`.
- Служебные ручки `GET /health`, `GET /handlers`, `GET /metrics`, исключённые из бизнес-метрик.
- Prometheus Histogram для latency и Counter для traffic/errors.
- Два экземпляра сервиса с различающимися `POD_ID` и `DATACENTER`.
- Prometheus с интервалом скрейпинга 5 секунд.
- Grafana с автоматически provisioned datasource UID `prometheus-ds`.
- Генератор Grafana dashboard, который получает маршруты по `GET /handlers`, а не хранит список endpoint-ов в коде.
- Healthcheck для сервисов и упорядоченный запуск через `depends_on`.
- wrk2 - эксперимент с нагрузочным тестированием

## Запуск

В корне репозитория выполните:

```bash
cp .env.example .env
```

В `.env` замените `GF_SECURITY_ADMIN_PASSWORD` на собственный пароль.
Запустите docker compose:

```bash
docker compose up --build -d
```

После первой сборки для повторного запуска можно использовать ровно `docker compose up`; при изменении исходников или Dockerfile пересоберите образы командой `docker compose up --build`.

Compose собирает образы и запускает зависимости. `dashboard-generator` ждёт успешных healthcheck у обоих приложений, Prometheus и Grafana; `load-tester` начинает работу только после готовности обоих приложений и успешного завершения `dashboard-generator` (`service_completed_successfully`).

После запуска:

- Grafana: <http://127.0.0.1:3000>
- Prometheus: <http://127.0.0.1:9090>
- Приложение в dc-1 и pod-1: <http://127.0.0.1:8081>
- Приложение в dc-2 и pod-2: <http://127.0.0.1:8082>

Войти в Grafana можно данными `GF_SECURITY_ADMIN_USER` (admin) и `GF_SECURITY_ADMIN_PASSWORD`.

Чтобы остановить сервисы и удалить также сохранённые временные ряды Prometheus и данные Grafana:

```bash
docker compose down -v
```

## Сервисы и порты

| Сервис | Назначение | Порт в контейнере | Порт хоста по умолчанию |
|---|---|---:|---:|
| `app_dc1` | HTTP API, `pod-1`, `dc-1` | `8080` | `8081` |
| `app_dc2` | HTTP API, `pod-2`, `dc-2` | `8080` | `8082` |
| `prometheus` | Сбор и запрос метрик | `9090` | `9090` |
| `grafana` | Dashboard и визуализация | `3000` | `3000` |
| `dashboard-generator` | Одноразовая публикация dashboard в Grafana REST API | исходящие HTTP-запросы | не публикует порт |
| `load-tester` | Нагрузочное тестирование | исходящие HTTP-запросы | не публикует порт |

## REST API микросервиса

| Метод и путь | Описание | Типичный успешный статус |
|---|---|---:|
| `POST /items` | Создаёт JSON-запись. Если поле `id` не передано, генерируется ID. | `201` |
| `GET /items/{id}` | Возвращает запись по ID или `404`, если её нет. | `200` |
| `PUT /items/{id}` | Обновляет существующую запись, ID берётся из URL; при отсутствии записи — `404`. | `200` |
| `GET /health` | Проверка готовности сервиса. | `200` |
| `GET /handlers` | JSON-массив бизнес-ручек `{method, path}`. Используется генератором dashboard. | `200` |
| `GET /metrics` | Экспорт метрик Prometheus в exposition-формате. | `200` |

Примеры запросов:

```bash
curl -s -w "\n" http://localhost:8081/health 
curl -s http://localhost:8081/handlers
curl -s http://localhost:8081/metrics

curl -i -X POST http://localhost:8081/items \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo"}'

curl -i http://localhost:8081/items/1
curl -i -X PUT http://localhost:8081/items/1 \
  -H 'Content-Type: application/json' \
  -d '{"name":"updated"}'
```

## Метрики, типы и лейблы

Метрики приложения собираются middleware только для бизнес-ручек. `GET /health`, `GET /handlers` и `GET /metrics` сами в эти метрики не попадают.

Лейблы в метриках:

| Лейбл | Описание | Пример |
|---|---|---|
| `pod_id` | Идентификатор пода из `POD_ID` | `pod-1` |
| `datacenter` | Идентификатор датацентра из `DATACENTER` | `dc-1` |
| `path` | Нормализованный шаблон пути | `/items/{id}` |
| `method` | HTTP-метод | `GET` |
| `status_code` | HTTP-код ответа | `200`, `404`, `500` |


### 1. Latency — Histogram

`http_server_requests_latency` — гистограмма времени обработки бизнес-запроса в секундах. Когда сервис публикует метрики, Prometheus видит их как три отдельных временных ряда:

- `http_server_requests_latency_bucket` — накопительное число наблюдений с latency `<= le`;
- `http_server_requests_latency_sum` — сумма длительностей;
- `http_server_requests_latency_count` — количество наблюдений.

Бакеты заданы в секундах: `0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10`.

### Почему Histogram, а не Summary или Gauge

Для агрегации latency между несколькими подами подходит **Histogram**.

- **Histogram** экспортирует накопительные бакеты с общим соглашением по границам `le`. Их значения можно суммировать между экземплярами, сохраняя распределение наблюдений в приближённом виде. `histogram_quantile()` вычисляет оценку квантиля из уже объединённого распределения. Нельзя сначала вычислить, например, p95 отдельно для каждого пода, а затем сложить или усреднить p95. 

### 2. Traffic / Throughput — Counter

`http_server_requests_count` — монотонный счётчик всех бизнес-запросов, включая ответы `2xx`, `4xx` и `5xx`. 

### 3. Errors — Counter по HTTP-коду

Отдельной метрики для ошибок нет. Ошибки считаются на основе общего счетчика http_server_requests_count с помощью фильтрации по лейблу status_code=~"[45]..".

## Генерация Grafana dashboard

`dashboard-generator` запрашивает `GET /handlers` у `app_dc1`, строит по три панели на каждую обнаруженную бизнес-ручку и публикует JSON через `POST /api/dashboards/db`. Поэтому добавление новой ручки в сервис и перезапуск стека приводит к появлению её панелей без изменений генератора.

У дашборда:

- фиксированный UID `my-service-metrics`;
- datasource с UID `prometheus-ds`;
- `editable: false`;
- переменные `datacenter` и `pod_id` с multi-select и опцией `All`;
- панели latency p50/p95/p99, RPS и ошибок по статус-коду для каждой бизнес-ручки.

Переменные выбирают лейблы Prometheus, а выражения используют regex-подстановки `${datacenter:regex}` и `${pod_id:regex}` для корректного multi-select / All.

## Нагрузочный тест (Go load-tester)

Запускается Go-утилита (load-tester) и генерирует ~40 RPS суммарно на оба пода (по 20 RPS), плавно наращивает нагрузку и генерирует 400/404/500 ошибки.

## Нагрузочный тест с wrk2

Дополнительно был проведен стресс-тест с помощью wrk2 на один под с интенсивностью 200 RPS.

```bash
wrk2 -t2 -c100 -R200 -d30s --latency -s ./load_tester_wrk2/load_script.lua http://localhost:8081
```

Результаты в директории ./load_tester_wrk2/results
