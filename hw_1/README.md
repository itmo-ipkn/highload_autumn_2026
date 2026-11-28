# Highload HW #1 — REST golden signals, Prometheus and auto-generated Grafana

Состав: **FastAPI** (два независимых экземпляра), **prometheus_client** (ASGI middleware),
**Prometheus**, **Grafana**, CLI-генератор дашборда и асинхронный генератор нагрузки.

## Быстрый запуск

Нужны Docker Engine + Docker Compose V2:

```bash
cp .env.example .env
docker compose up --build
```

### Адреса

| Адрес | Назначение |
|---|---|
| <http://localhost:3000/d/items-http-metrics> | Дашборд Grafana |
| <http://localhost:3000> | Grafana (логин и пароль из `.env`) |
| <http://localhost:9090/targets> | Prometheus Targets: оба экземпляра `UP` |
| <http://localhost:8001/health> | Экземпляр `pod-1` (`dc-east`) |
| <http://localhost:8002/health> | Экземпляр `pod-2` (`dc-west`) |
| <http://localhost:8001/handlers> | Автоматический список API-ручек |
| <http://localhost:8001/metrics> | Prometheus exposition format |

В `.env.example` содержится только **публичное учебное значение пароля**;
это не действующий производственный секрет. Перед использованием за пределами
локального компьютера выберите собственный сильный пароль в `.env`.
Все опубликованные порты привязаны к `127.0.0.1` хоста.

## API

Бизнес-ручки:

| Метод | Путь | Результат |
|---|---|---|
| `POST` | `/items` | `201` + JSON с `id`, `name`, `value` |
| `GET` | `/items/{id}` | `200` или `404`, если ID не существует |
| `PUT` | `/items/{id}` | `200` или `404` при неизвестном ID |

Служебные `GET /health`, `GET /metrics`, `GET /handlers` исключены из бизнес-метрик.

Пример создания:

```bash
curl -sS -X POST http://localhost:8001/items \
  -H 'Content-Type: application/json' \
  -d '{"name":"example","value":42}'
```

Данные хранятся в `OrderedDict` в оперативной памяти конкретного экземпляра;
общей БД нет. При остановке контейнера они теряются, данные `pod-1` не видны `pod-2`.
Чтобы многосуточный генератор нагрузки не расходовал память бесконечно,
хранилище ограничено последними 5000 объектами на экземпляр.

Контролируемый режим проверки 500 (`POST /items` с полем
`"name":"__simulate_500__"`) возвращает HTTP 500; это не отдельная ручка.
Генератор нагрузки использует его в небольшом проценте запросов.

## Метрики и принципы инструментирования

Инструментирование выполнено через собственный ASGI middleware в
`app/telemetry.py` на основе `prometheus_client`. Метрики формируются после
сопоставления маршрута FastAPI, поэтому лейбл `route` содержит шаблон, а не
фактические значения `{id}` — это защищает от высокой кардинальности.

| Имя | Тип Prometheus | Лейблы | Назначение |
|---|---|---|---|
| `http_request_duration_seconds` | Histogram | `pod_id`, `datacenter`, `method`, `route` | Задержка запросов в секундах; экспорт `_bucket`, `_sum`, `_count` |
| `http_requests_total` | Counter | `pod_id`, `datacenter`, `method`, `route`, `status_code` | Счётчик запросов, RPS и ошибок 4xx/5xx |

Для каждого экземпляра значения `pod_id` и `datacenter` получаются строго из
переменных окружения `POD_ID` и `DATACENTER` (в compose соответственно
`pod-1 / dc-east` и `pod-2 / dc-west`). Отдельные метрики на ответ не создаются:
код ответа представлен лейблом только для Counter.

### Почему Histogram, а не Summary?

Для p50 / p95 / p99 нужна единая квантилизация после объединения запросов всех реплик.
У Summary вычисленные квантили нельзя корректно усреднять между подами:
среднее p95 двух подов не равно общему p95. Классический Histogram отдаёт
кумулятивные счетчики в одинаковых бакетах (`le`) — их можно суммировать по
репликам, а затем вызвать `histogram_quantile`. Такой подход даёт статистически
корректную агрегацию распределений при неизбежной погрешности интерполяции
внутри бакета. Бакеты в проекте адаптированы под короткие HTTP-запросы:
от 1 мс до 10 секунд, плюс `+Inf`.

RPS складывается по выбранным подам и ответам:

```promql
sum(rate(http_requests_total{
  method="GET", route="/items/{id}",
  datacenter=~"${datacenter:regex}", pod_id=~"${pod_id:regex}"
}[$__rate_interval]))
```

Ошибки отображаются отдельными линиями по коду (ошибок в секунду):

```promql
sum by (status_code) (rate(http_requests_total{
  method="GET", route="/items/{id}",
  datacenter=~"${datacenter:regex}", pod_id=~"${pod_id:regex}",
  status_code=~"4..|5.."
}[$__rate_interval]))
```

## Как строится дашборд

1. Docker Compose запускает оба экземпляра и проверяет их `/health`.
2. Prometheus (образ `prom/prometheus:v3.2.1`) опрашивает оба адреса
   каждые 5 секунд; после его readiness запускается Grafana.
3. Grafana (образ `grafana/grafana:11.6.0`) автоматически создаёт Prometheus
   datasource с фиксированным `uid: prometheus-main` через provisioning YAML.
4. `dashboard-generator` после health checks забирает `GET /handlers`,
   строит JSON с *ремя панелями для каждой ручки (latency p50/p95/p99,
   RPS и 4xx/5xx по кодам) и отправляет его в
   `POST /api/dashboards/db` с `overwrite: true`. У дашборда
   `uid: items-http-metrics` и `editable: false`.
5. Только после успешного завершения генератора (`service_completed_successfully`)
   запускается долговременная нагрузка.

## Нагрузка

`load-generator` обращается напрямую к `app-dc1:8000` и `app-dc2:8000`:

- Постепенно увеличивает запросы от 3 до 36 RPS суммарно за 120 секунд,
  затем поддерживает примерно 36 RPS до остановки compose.
- Распределяет запросы поровну между двумя репликами.
- Использует все три бизнес-ручки (30% POST, 40% GET, 30% PUT).
- В каждом наборе из 100 запросов генерирует 2% 404 (запросы неизвестных
  ID) и 2% 500 (предусмотренный режим ошибки POST). Каждый тип ошибки
  возникает на обеих репликах. Остальные операции выполняются успешно,
  кроме возможных временных сетевых сбоев.
- Запоминает отдельно последние созданные ID на каждом поде; не обращается
  с ID одной реплики к другой и поддерживает постоянное время работы.

Для проверки:

```bash
docker compose ps
docker compose logs -f dashboard-generator load-generator
curl -fsS http://localhost:8001/handlers
curl -fsS http://localhost:8001/metrics | grep 'http_requests_total'
```

### Остановка

```bash
docker compose down
```

## Тесты и структура

Для локальных unit/integration тестов без Docker:

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q
```