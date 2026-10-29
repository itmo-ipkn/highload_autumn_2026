# Items — автоматизация мониторинга в Grafana

Два экземпляра одного Go-сервиса, метрики latency, RPS и ошибок, дашборд Grafana по живому списку ручек и фоновая нагрузка k6.

Оригинальный репозиторий с коммитами тут github.com:ekubyshin/highload-lab1.git

## Запуск

Нужны Docker и Docker Compose.

```sh
cp .env.example .env
docker compose up
```

Пароль в `.env` можно не заполнять: при первом запуске он генерируется и сохраняется в volume. Через 2–3 минуты на дашборде есть данные.

| Адрес                                          | Что это                                     |
| ---------------------------------------------- | ------------------------------------------- |
| [http://127.0.0.1:9091](http://127.0.0.1:9091) | `app-1`, `POD_ID=1`, `DATACENTER=dc-1`      |
| http://127.0.0.1:8082                          | `app-2`, `POD_ID=2`, `DATACENTER=dc-2`      |
| http://127.0.0.1:9090                          | Prometheus, job `items-app`, оба экземпляра |
| http://127.0.0.1:3000/d/items-service          | Дашборд `items-service`                     |

Логин Grafana:

```sh
docker run --rm -v highload-lab1_credentials:/credentials:ro alpine:3.24.3 \
  sh -c 'printf "user: %s\npassword: %s\n" "$(cat /credentials/username)" "$(cat /credentials/password)"'
```

`.env` в Git не входит, пример — в `.env.example`. Свой пароль задаётся в `.env` до первого запуска. Позже смена `.env` пароль не меняет. `docker compose down -v` удаляет его вместе с данными Grafana.

`dashboard-generator` стартует после healthy приложений и Grafana, читает `GET /handlers` и записывает дашборд. k6 стартует после успешного завершения генератора.

## API

Хранилище — `sync.Map` в памяти процесса. У подов разные данные. ID назначает сервис. Тело `POST` и `PUT`: `{"name":"...","value":"..."}`.

| Метод и путь | Успех | Ошибка |
|---|---|---|
| `POST /items` | `201` | `400` |
| `GET /items/{id}` | `200` | `404` |
| `PUT /items/{id}` | `200` | `404`, `400` |
| `GET /health` | `200` | — |
| `GET /metrics` | текст Prometheus | — |
| `GET /handlers` | каталог бизнес-ручек | — |

```json
{
  "handlers": [
    {"method": "POST", "path": "/items"},
    {"method": "GET", "path": "/items/{id}"},
    {"method": "PUT", "path": "/items/{id}"}
  ]
}
```

## Метрики

`/health` и `/metrics` не учитываются.

| Метрика | Тип | Лейблы |
|---|---|---|
| `http_requests_total` | Counter | `pod_id`, `datacenter`, `method`, `route`, `status_code` |
| `http_request_duration_seconds` | Histogram | `pod_id`, `datacenter`, `method`, `route` |

`pod_id` и `datacenter` берутся из `POD_ID` и `DATACENTER` и не меняются за жизнь процесса. `method` и `route` берутся из регистрации ручки: шаблон `/items/{id}`, а не фактический путь и не UUID. Иначе каждый ID дал бы отдельный ряд. Код ответа есть только у счётчика. Отдельной метрики ошибок нет: панель ошибок — срез того же счётчика по `status_code=~"4..|5.."`.

### Почему Histogram

Нужны перцентили по нескольким экземплярам сразу. Summary считает квантиль внутри процесса и отдаёт готовое число. Среднее двух p95 не равно p95 объединённых запросов: квантиль — позиция в распределении, а распределение при отдаче готового числа уже потеряно.

Histogram отдаёт кумулятивные счётчики `_bucket` с общей границей `le`. Счётчики складываются. Сначала `sum by (le)` по выбранным подам и датацентрам, затем `histogram_quantile`. Оценка общая для выбранного набора экземпляров. Точность ограничена шириной бакета: значение интерполируется внутри интервала.

Границы одинаковые у всех экземпляров и не читаются из окружения. Клиент Prometheus сам добавляет `+Inf`. Конечные границы в секундах:

```text
0.0000005, 0.000001, 0.0000025, 0.000005,
0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005,
0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
0.1, 0.25, 0.5, 1, 2.5, 5
```

Нижняя граница `500ns`: метрика измеряет время обработчика в процессе, без сети. У in-memory ручек это микросекунды. При первой границе `100µs` почти все наблюдения попадали в один бакет, и перцентили переставали различаться.

Пример для `POST /items`. Для `GET` и `PUT` меняются `method` и `route`.

```promql
histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket{job="items-app",method="POST",route="/items",datacenter=~`${datacenter:regex}`,pod_id=~`${pod_id:regex}`}[$__rate_interval])))

sum(rate(http_requests_total{job="items-app",method="POST",route="/items",datacenter=~`${datacenter:regex}`,pod_id=~`${pod_id:regex}`}[$__rate_interval]))

sum by (status_code) (rate(http_requests_total{job="items-app",method="POST",route="/items",datacenter=~`${datacenter:regex}`,pod_id=~`${pod_id:regex}`,status_code=~"4..|5.."}[$__rate_interval]))
```

RPS — сумма скоростей выбранных экземпляров, не среднее. Перцентили считаются по сумме бакетов, а не усредняются между подами. `rate` берётся до суммирования, чтобы перезапуск пода не ломал расчёт.

## Дашборд

Генератор не содержит списка ручек. На каждую ручку из `GET /handlers` создаются панели latency (p50, p95, p99), RPS и ошибок по коду. Переменные `datacenter` и `pod_id` — multi-select и All. Дашборд записан с `editable: false`. Новая ручка появляется после регистрации в приложении и `docker compose up --build`, без правок генератора.

## Нагрузка

k6 поднимает суммарную нагрузку на оба пода с 5 до 30 RPS за 120 секунд и держит 30 RPS, пока стек не остановят. Запросы идут на все три ручки. Около 2% ответов — `404` (несуществующий id), около 2% — `400` (некорректное тело).

## Версии

Образы зафиксированы в `.env.example`, тега `latest` нет: Prometheus `v3.15.1`, Grafana `13.3.0`, k6 `1.6.0`. Дашборд собирается `grafana-foundation-sdk/go` `v0.0.22-0.20261027120448-78572dabdce6`.
