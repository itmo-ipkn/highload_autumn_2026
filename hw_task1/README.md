# Items Service — DDIA HW1 (ФиБ track)

In-memory CRUD-сервис на C++ с метриками Prometheus, автогенерацией дашборда
Grafana и нагрузочным тестом k6.

## Что это

| Метод | Путь | Назначение |
|---|---|---|
| `POST` | `/items` | создать запись |
| `GET` | `/items/{id}` | получить (404, если нет) |
| `PUT` | `/items/{id}` | обновить |
| `GET` | `/health` | liveness |
| `GET` | `/metrics` | метрики Prometheus |
| `GET` | `/handlers` | список ручек для генератора дашборда |

Хранилище — `std::map<std::uint64_t, Item>` под `std::mutex`.

## Запуск

Нужны Docker 24+ (Compose v2) и Python 3.10+.

```bash
git clone git@github.com:MickeyRU/Highload_HW_1.git
cd Highload_HW_1

cp .env.example .env
python3 scripts/configure.py     # генерирует пароль Grafana
docker compose up
```

Дашборд: <http://localhost:3000/d/items-service>, логин `admin`,
пароль — `GRAFANA_ADMIN_PASSWORD` из `.env`:

```bash
grep ^GRAFANA_ADMIN_PASSWORD .env
```

`configure.py` вызывается отдельно, потому что Compose читает `.env`
до создания контейнеров — сгенерировать секрет изнутри стека нельзя.

Посмотреть, что происходит:

```bash
docker compose ps                     # статусы сервисов
docker compose logs -f load           # прогресс k6
docker compose logs dashboard-generator
```

Остановка: `docker compose down` (с `-v` — вместе с томами).

## Порты

| Порт | Что |
|---|---|
| `8080` | `app1` (`dc-1`) |
| `8081` | `app2` (`dc-2`) |
| `3000` | Grafana |
| `9090` | Prometheus |

Приложения и Grafana привязаны к `127.0.0.1`; Prometheus — к
`${PROMETHEUS_PORT}` из `.env`.

## Стек

| Сервис | Образ |
|---|---|
| `app1`, `app2` | `items-service:1.0.0` (сборка из `Dockerfile`) |
| `prometheus` | `prom/prometheus:v3.15.0` |
| `grafana` | `grafana/grafana:13.2.4` |
| `dashboard-generator` | `python:3.13-slim` |
| `load` | `grafana/k6:1.3.0` |

Два экземпляра приложения запускаются из одного образа и различаются
только `POD_ID` и `DATACENTER`. Версии C++-зависимостей зафиксированы
через `builtin-baseline` в `vcpkg.json` и `ARG VCPKG_COMMIT` в `Dockerfile`.

## Метрики

Приложение экспортирует две метрики:

| Метрика | Тип | Сигнал |
|---|---|---|
| `http_request_duration_seconds` | histogram | latency |
| `http_requests_total` | counter | RPS и errors |

Из них строятся три золотых сигнала:

| Сигнал | PromQL |
|---|---|
| Latency | `histogram_quantile(φ, sum by (le) (rate(http_request_duration_seconds_bucket{…}[$__rate_interval])))` |
| RPS | `sum(rate(http_requests_total{…}[$__rate_interval]))` |
| Errors | `sum by (status) (rate(http_requests_total{…,status=~"4..\|5.."}[$__rate_interval]))` |

## Лейблы

| Лейбл | Значения | Источник |
|---|---|---|
| `pod` | `pod-1`, `pod-2` | env `POD_ID` |
| `datacenter` | `dc-1`, `dc-2` | env `DATACENTER` |
| `method` | `GET`, `POST`, `PUT` | маршрут |
| `route` | `/items`, `/items/{id}` | шаблон из `kApiHandlers` |
| `status` | `200`, `201`, `400`, `404`, `500` | код ответа |

`route` — шаблон, а не фактический путь. Иначе каждый id создавал бы
отдельную серию: `/items/1`, `/items/2`, … — прямая дорога к
cardinality explosion.

Служебные `/health`, `/metrics`, `/handlers` в метрики не попадают.

## Почему histogram, а не summary

Задание требует корректной агрегации перцентилей по нескольким
экземплярам. Квантиль — это позиция в упорядоченном распределении,
поэтому квантиль объединения не выводится из квантилей выборок.

Пример: `pod-1` — 1000 запросов, 5% медленнее 10 мс, p95 = 1 мс;
`pod-2` — 1000 запросов, 7% медленнее 10 мс, p95 = 1 мс. В объединённой
выборке медленных 6% — больше 5%, значит p95 объединения попадает в
медленные запросы. Из «1 мс» и «1 мс» получить p95 объединения нельзя.

Summary считает квантили на клиенте и отдаёт готовые числа — распределение
внутри выборки теряется, сложить такие метрики невозможно.

Histogram хранит кумулятивные счётчики попаданий по бакетам (`le`).
Счётчики аддитивны, поэтому `sum by (le)` даёт точную гистограмму
объединения, а `histogram_quantile` считает по ней общий перцентиль:

```promql
histogram_quantile(0.95,
  sum by (le) (rate(http_request_duration_seconds_bucket{…}[$__rate_interval])))
```

`sum by (le)` обязателен **внутри** `histogram_quantile`, иначе каждый под
посчитается отдельно.

**Бакеты** — 18 границ от 2 мкс до 1 с. Первая версия начиналась
со 100 мкс, и `histogram_quantile` возвращал `NaN`: все наблюдения
попадали в первый бакет. Плотность в микросекундной области соответствует
реальным задержкам сервиса (p50 ≈ 20 мкс, p95 ≈ 50 мкс); границы до 1 с
оставлены для деградации и таймаутов.

## Дашборд

`scripts/generate_dashboard.py` читает `GET /handlers`, строит по три
панели на ручку (latency, RPS, errors) и отправляет в Grafana API.
Список ручек в скрипте не хранится — новая ручка появится на дашборде
после `docker compose up`.

Дашборд создаётся с `editable: false` и фиксированным `uid`, поэтому
перезапуск перезаписывает его, а не плодит копии.

## Нагрузка

`load/test.js` — k6, два сценария `ramping-arrival-rate`: рост до 25 RPS
на под за 2 минуты, затем фон 30 минут (50 RPS суммарно). Распределение:
40% `POST`, 35% `GET`, 20% `PUT`, 5% `GET /items/999999`.

Ошибка 500 генерируется сервером по `FAILURE_RATE` (0.05), а не клиентом.
Запросы идут с `Connection: close` — иначе потоки `cpp-httplib` остаются
привязаны к keep-alive соединениям, и `/health` перестаёт отвечать
под нагрузкой.

## Секреты

`.env.example` содержит плейсхолдер, `scripts/configure.py` заменяет его
на `secrets.token_hex(32)` и ставит права `0600`. `.env` в `.gitignore`.

## Известные ограничения

- `FAILURE_RATE=0.05` включён для демонстрации ошибок на дашборде.
- Данные in-memory и теряются при перезапуске; при нагрузке хранилище
  растёт (~36 тыс. записей за 30 минут).
- Образ собирается под архитектуру хоста.

## Структура

```
src/            C++: main, store, metrics, exposition, http_metrics, failure, handlers
monitoring/     конфиги Prometheus и provisioning Grafana
scripts/        configure.py, generate_dashboard.py
load/           test.js (k6)
compose.yaml    стек: app1, app2, prometheus, grafana, generator, load
```