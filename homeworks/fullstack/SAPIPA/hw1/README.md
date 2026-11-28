# Инструменты автоматизации в Grafana

Работа 1 по DDIA: C++20/userver-сервис с in-memory хранилищем, Prometheus, Grafana и генераторы дашборда и нагрузки на Go.

## Запуск

Нужны Docker и Docker Compose v2. Все компоненты собираются и запускаются автоматически:

```bash
cp .env.example .env
docker compose up
```

После первоначальной сборки и запуска контейнеров данные появляются за 2–3 минуты. Дашборд: <http://localhost:3000/d/userver-crud-monitoring>. Логин и пароль Grafana задаются в `.env` переменными `GRAFANA_ADMIN_USER` и `GRAFANA_ADMIN_PASSWORD`. Файл `.env` исключён из Git, в `.env.example` приведены демонстрационные значения.

| Компонент | Адрес |
|---|---|
| Первый экземпляр API | <http://localhost:8080> |
| Его метрики | <http://localhost:8081/metrics> |
| Второй экземпляр API | <http://localhost:8082> |
| Его метрики | <http://localhost:8083/metrics> |
| Prometheus | <http://localhost:9090> |
| Grafana | <http://localhost:3000> |

Остановка: `docker compose down`. После изменений исходников: `docker compose up --build`.

## API и мониторинг

| Метод | Путь | Назначение |
|---|---|---|
| POST | `/items` | Создать запись, ответ 201 |
| GET | `/items/{id}` | Получить запись, 404 для отсутствующего ID |
| PUT | `/items/{id}` | Обновить запись, 404 для отсутствующего ID |

POST и PUT принимают JSON `{"name":"example"}`. Некорректные данные дают 400.

Служебные ручки: `/health` — готовность, `/handlers` — список бизнес-ручек, `/metrics` — метрики на отдельном monitor-порту. HTTP middleware userver измеряет бизнес-запросы, включая ответы 4xx/5xx, служебные ручки исключены.

| Метрика | Тип и назначение |
|---|---|
| `http_request_total` | Counter: число запросов, RPS вычисляется через `rate()` |
| `http_request_errors_total` | Counter: число ответов 4xx/5xx по коду `status` |
| `http_request_duration_seconds` | Histogram: длительности в секундах, экспорт `_bucket`, `_count`, `_sum` |

Лейблы: `pod_id` из `POD_ID`, `datacenter` из `DATACENTER`, `method` и `route` с шаблоном пути (`/items/{id}`, а не фактическим ID). Ошибки дополнительно имеют `status`, бакеты — `le`.

Для latency выбран **Histogram**, потому что его бакеты можно суммировать между экземплярами, сохраняя корректную агрегацию p50/p95/p99. Сначала вычисляется `rate()` бакетов, затем `sum by (le)`, после чего `histogram_quantile()`. Усреднение готовых перцентилей отдельных подов некорректно.

```promql
histogram_quantile(0.95, sum by (le) (
  rate(http_request_duration_seconds_bucket{
    method="GET",route="/items/{id}"
  }[1m])
))
```

## Дашборд и нагрузка

Два экземпляра одного образа работают с разными `POD_ID` и `DATACENTER`. Prometheus опрашивает оба каждые 5 секунд. Datasource Grafana создаётся через provisioning с UID `prometheus-main`.

Генератор получает маршруты из `/handlers` после healthcheck приложений и Grafana. Для каждой ручки создаются панели p50/p95/p99, RPS и ошибок по кодам. RPS и ошибки суммируются, перцентили вычисляются по суммарным бакетам. Дашборд нередактируемый, повторная генерация обновляет его по тому же UID.

Нагрузчик запускается после успешного завершения генератора, охватывает оба экземпляра и все ручки, за 180 секунд увеличивает суммарную нагрузку с 5 до 30 RPS и поддерживает её до остановки. На каждый цикл из 100 запросов каждого экземпляра запланированы 2 ответа 404 и 1 ответ 500. Код 500 вызывается запросом `/items/0?simulate_error=1` при `ENABLE_TEST_ERRORS=1`.

В `.env` можно изменить `LOAD_TARGET_RPS`, `LOAD_RAMP_SECONDS`, `POD_1_ID`/`POD_2_ID`, `DATACENTER_1`/`DATACENTER_2` и `ENABLE_TEST_ERRORS`.
