# DDIA HW1 — мониторинг CRUD-сервиса

Небольшой FastAPI-сервис с хранением данных в памяти. В Compose запускаются два экземпляра приложения, Prometheus, Grafana и генератор нагрузки.

## Запуск

Нужны Docker Engine и Docker Compose. Из каталога проекта:

```sh
cp .env.example .env
docker compose up
```

Пароль Grafana берётся из `.env`. Файл игнорируется Git; перед использованием можно заменить пример пароля на свой. Дашборд создаётся автоматически, данные на графиках появляются по мере разгона нагрузки.

| Адрес | Что открывает |
| --- | --- |
| http://localhost:13030/d/ddia-hw1-items/ddia-hw1-c2b7-http-handlers | Дашборд Grafana |
| http://localhost:19090 | Prometheus |
| http://localhost:18081/health | Первый экземпляр приложения (dc-1) |
| http://localhost:18082/health | Второй экземпляр приложения (dc-2) |
| http://localhost:18081/metrics | Метрики первого экземпляра |
| http://localhost:18082/metrics | Метрики второго экземпляра |

Для остановки: `docker compose down`.

## API

Данные каждого экземпляра хранятся отдельно и исчезают при его перезапуске.

| Метод | Путь | Результат |
| --- | --- | --- |
| POST | `/items` | Создаёт запись, возвращает 201 и её ID |
| GET | `/items/{id}` | Возвращает запись или 404 |
| PUT | `/items/{id}` | Заменяет запись или возвращает 404 |

Пример создания записи:

```sh
curl -X POST http://localhost:18081/items \
  -H 'Content-Type: application/json' \
  -d '{"name":"example","description":"optional"}'
```

Служебные ручки: `GET /health`, `GET /metrics` и `GET /handlers`. Последняя отдаёт методы и шаблоны путей, которые попадают в мониторинг.

## Метрики

Middleware собирает метрики для трёх CRUD-ручек и `GET /handlers`. Запросы к `/health` и `/metrics` не учитываются.

| Метрика | Тип | Метки |
| --- | --- | --- |
| `items_http_requests_total` | Counter | `pod_id`, `datacenter`, `method`, `route`, `status_code` |
| `items_http_request_duration_seconds` | Histogram | `pod_id`, `datacenter`, `method`, `route`; у бакетов также `le` |

`pod_id` и `datacenter` задаются переменными окружения `POD_ID` и `DATACENTER`. В `route` записывается шаблон `/items/{id}`, а не конкретный ID, чтобы число временных рядов не росло с каждым запросом. Код ответа нужен в счётчике для разбивки ошибок по 404, 422 и другим статусам.

Для latency выбран Histogram: бакеты разных экземпляров можно сложить и затем вычислить общие p50, p95 и p99. Клиентские квантили Summary так объединять нельзя. Границы бакетов у обоих экземпляров одинаковые — от 0,5 мс до 10 с.

Примеры PromQL для обоих экземпляров:

```promql
sum by (method, route) (rate(items_http_requests_total[1m]))

sum by (method, route, status_code) (
  rate(items_http_requests_total{status_code=~"4..|5.."}[1m])
)

histogram_quantile(0.95,
  sum by (le, method, route) (
    rate(items_http_request_duration_seconds_bucket[5m])
  )
)
```

В последнем запросе `0.95` можно заменить на `0.50` или `0.99`. Для фильтрации добавьте `pod_id` и `datacenter` в селектор метрики.

## Дашборд

После проверки готовности приложений и Grafana скрипт `scripts/generate_dashboard.py` получает список ручек через `/handlers` и создаёт дашборд через API Grafana. Ручки в скрипте не перечислены: новая ручка появится на дашборде после перезапуска стека. Datasource Prometheus создаётся через provisioning с фиксированным UID.

Для каждой ручки есть три панели: p50/p95/p99 latency, RPS и ошибки по кодам ответа. Селекторы Datacenter и Pod поддерживают несколько значений и All. RPS суммируется по выбранным экземплярам, а перцентили считаются по сумме бакетов. Редактирование дашборда отключено.

## Нагрузка

`scripts/load.py` запускается после успешного завершения генератора дашборда. Нагрузка растёт с 2 до 30 RPS суммарно за 150 секунд и затем держится до остановки Compose. Запросы идут на оба экземпляра и затрагивают все четыре наблюдаемые ручки.

В каждом цикле из 50 запросов один даёт 404 (несуществующий ID), ещё один — 422 (пустое имя). Ожидаемая доля ошибок — 4%. Текущую скорость и коды ответов можно посмотреть так:

```sh
docker compose logs -f load_generator
```
