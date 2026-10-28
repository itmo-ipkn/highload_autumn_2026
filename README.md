# DDIA HW-1 — Инструменты автоматизации в Grafana

Каркас pet-project по заданию `ITMO-DDIA-2026-DevOps_ФиБ-HW-1.md`: CRUD-микросервис на Spring Boot (in-memory хранилище) + docker-compose с Prometheus и Grafana.

По условию задания с помощью AI выполняется несодержательная часть — она уже реализована в этом каркасе:

- CRUD-приложение (`POST /items`, `GET /items/{id}`, `PUT /items/{id}`, служебная `GET /health`);
- Dockerfile (multi-stage: Maven build → JRE-образ, non-root, HEALTHCHECK по `/health`);
- docker-compose.yaml: два экземпляра приложения из одного образа с разными `POD_ID`/`DATACENTER`, Prometheus с фиксированным конфигом, Grafana с provisioning datasource;
- проброс параметров через env-переменные и `.env`;
- заготовки сервисов `dashboard-generator` и `load-generator` в compose (закомментированы).

Самостоятельная часть (по условию её нужно выполнить самому, метрики/дашборд/нагрузка не реализованы) — см. [Что осталось сделать](#что-осталось-сделать-самостоятельную-часть).

## Быстрый старт

```bash
cp .env.example .env
docker compose up
```

Через 30–60 секунд все сервисы поднимаются. Порты по умолчанию (меняются в `.env`):

| Сервис      | Порт на хосте | Адрес                     |
|-------------|---------------|---------------------------|
| app1 (dc-1) | 8081          | http://localhost:8081     |
| app2 (dc-2) | 8082          | http://localhost:8082     |
| Prometheus  | 9090          | http://localhost:9090     |
| Grafana     | 3000          | http://localhost:3000     |

Grafana: логин/пароль администратора берутся из `.env` (`GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD`). Datasource `Prometheus` создаётся автоматически (uid `prometheus-ddia`).

Проверка CRUD вручную:

```bash
curl -s -X POST localhost:8081/items -H 'Content-Type: application/json' -d '{"value":"first"}'
curl -s localhost:8081/items/<id>
curl -s -X PUT localhost:8081/items/<id> -H 'Content-Type: application/json' -d '{"value":"updated"}'
curl -s -o /dev/null -w '%{http_code}\n' localhost:8081/items/missing   # 404
```

## Структура репозитория

```
app/                          Spring Boot CRUD-сервис
  Dockerfile
  src/main/java/com/example/items/
    ItemsApplication.java
    model/Item.java           запись хранилища (record)
    storage/InMemoryItemStore.java   ConcurrentHashMap, атомарный update через compute()
    web/ItemController.java   POST/GET/PUT /items
    web/HealthController.java GET /health (+ показывает POD_ID/DATACENTER)
    web/ApiExceptionHandler.java  единый JSON-формат ошибок
prometheus/prometheus.yml     скрейп обоих экземпляров (targets app1:8080, app2:8080)
grafana/provisioning/datasources/prometheus.yml   datasource с фиксированным uid
docker-compose.yaml           весь стек
.env.example                  пример переменных окружения
```

## Переменные окружения

| Переменная              | Где используется | Назначение                                   |
|-------------------------|------------------|----------------------------------------------|
| `POD_ID`                | контейнер приложения | лейбл номера пода в метриках             |
| `DATACENTER`            | контейнер приложения | лейбл датацентра в метриках              |
| `GRAFANA_ADMIN_USER/PASSWORD` | Grafana и генератор дашборда | креды администратора (секрет из `.env`) |
| `*_PORT`                | docker-compose   | порты сервисов на хосте                      |

API приложения (`/metrics`, `/handlers`) и параметры генератора/нагрузочного инструмента добавляются в `.env` по мере реализации частей 1–3.

## Что осталось сделать (самостоятельная часть)

### Часть 1 — библиотека метрик

- [+] Перехват HTTP-запросов идиоматичным для Spring средством (middleware: `HandlerInterceptor` / `OncePerRequestFilter`)
- [+] Метрики latency / throughput / errors с лейблами пода, датацентра, ручки (`/items/{id}`, не фактический путь) и кода ответа
- [+] Ручка `GET /metrics` в формате Prometheus 
- [+] Описать в этом README названия метрик/лейблов и обосновать тип метрики для latency (тип должен корректно агрегировать перцентили 50/95/99 между экземплярами):
## Метрики HTTP и формат Prometheus

Приложение экспортирует HTTP-метрики через `GET /metrics` в формате Prometheus. Метрики собираются отдельно для каждого экземпляра приложения и агрегируются Prometheus.

### Метрики

| Метрика Prometheus | Тип | Описание |
|---|---|---|
| `http_requests_total` | Counter | Общее количество завершённых HTTP-запросов |
| `http_errors_total` | Counter | Количество запросов, завершившихся HTTP-статусом `4xx` или `5xx` |
| `http_request_duration_seconds` | Histogram | Распределение времени обработки HTTP-запросов в секундах |

Prometheus-экспозиция Histogram включает следующие временные ряды:

- `http_request_duration_seconds_bucket` — количество наблюдений, время которых не превышает границу `le`;
- `http_request_duration_seconds_sum` — суммарное время обработки запросов;
- `http_request_duration_seconds_count` — количество измеренных запросов.

### Лейблы

Все перечисленные метрики используют следующие лейблы:

| Лейбл | Описание | Пример |
|---|---|---|
| `pod` | Идентификатор экземпляра приложения из `POD_ID` | `app1` |
| `datacenter` | Идентификатор датацентра из `DATACENTER` | `dc-1` |
| `method` | HTTP-метод | `GET` |
| `uri` | Шаблон маршрута Spring MVC | `/items/{id}` |
| `status` | HTTP-код ответа | `200`, `404`, `500` |

В качестве `uri` используется шаблон маршрута, а не фактический URL. Например, запросы `/items/123` и `/items/456` агрегируются под одним значением `/items/{id}`. Это ограничивает кардинальность метрик и предотвращает создание отдельного временного ряда для каждого идентификатора.

Для запросов, которым не удалось сопоставить маршрут, значение `uri` может быть `UNKNOWN`.

### Почему Histogram используется для latency

Для latency используется **Histogram**, а не Summary с заранее вычисляемыми перцентилями.

Histogram экспортирует накопительные счётчики наблюдений для заданных границ `le`. Prometheus может суммировать эти счётчики между экземплярами приложения, а затем вычислять перцентили по объединённому распределению. Это позволяет корректно агрегировать p50, p95 и p99 для `app1` и `app2`, даже если нагрузка на них различается.

Summary, который экспортирует заранее рассчитанные перцентили на каждом экземпляре, не позволяет корректно объединять эти значения обычной операцией `sum` или `avg`. Например, среднее значение p95 двух экземпляров не является p95 общего распределения.

Для расчёта перцентилей используется `histogram_quantile()`:

```promql
histogram_quantile(
  0.50,
  sum by (le, pod, datacenter, uri, method) (
    rate(http_request_duration_seconds_bucket[5m])
  )
)
```

Для p95 и p99 замените `0.50` на `0.95` и `0.99` соответственно.

Чтобы получить общий перцентиль по обоим экземплярам, исключите `pod` и `datacenter` из группировки:

```promql
histogram_quantile(
  0.95,
  sum by (le, uri, method) (
    rate(http_request_duration_seconds_bucket[5m])
  )
)
```

Таким образом, перцентиль вычисляется по объединённым bucket-счётчикам, а не усредняется между экземплярами.

### Throughput и errors

Throughput вычисляется как скорость роста счётчика `http_requests_total`:

```promql
sum by (pod, datacenter, uri, method) (
  rate(http_requests_total[5m])
)
```

Количество ошибок 5xx в секунду:

```promql
sum by (pod, datacenter, uri) (
  rate(http_errors_total{status=~"5.."}[5m])
)
```

Для всех ошибок `4xx` и `5xx` используйте `http_errors_total` без фильтра по `status`.

### Ограничения

- `http_errors_total` считает ответы со статусом `4xx` и `5xx`; бизнес-ошибки, возвращённые с кодом `2xx`, в него не входят.
- Перцентиль по Histogram является оценкой на основе bucket-распределения; точность зависит от границ bucket.
- Лейблы с высокой кардинальностью, такие как фактический URL, идентификатор элемента, пользователь или request ID, не используются.
- `GET /metrics` исключён из измерений HTTP-запросов, чтобы скрейпинг Prometheus не создавал лишний поток метрик.


### Часть 2 — генератор дашборда

- [+] Ручка `GET /handlers` со списком ручек приложения
- [+] CLI-скрипт генерации дашборда через REST API Grafana: берёт список ручек в рантайме, создаёт панели p50/p95/p99, RPS, ошибки по кодам; селекторы датацентра и пода (multi-select + All); non-editable dashboard
- [+] Раскомментировать сервис `dashboard-generator` в docker-compose.yaml
- [+] Описать PromQL-шаблоны (sum для rate, histogram_quantile поверх суммы по le):
### 1. Latency: p50, p95, p99

Для вычисления перцентилей используется histogram-метрика `http_request_duration_seconds_bucket`.

Сначала применяем `rate()` к bucket-счётчикам, затем агрегируем их через `sum by (le, pod, datacenter)` и вычисляем квантиль с помощью `histogram_quantile()`.

Пример для p95:

```promql
histogram_quantile(
  0.95,
  sum by (le, pod, datacenter) (
    rate(http_request_duration_seconds_bucket{
      uri="/items/{id}",
      method="GET",
      datacenter=~"$datacenter",
      pod=~"$pod"
    }[5m])
  )
)
```

Для p50 замените `0.95` на `0.50`, для p99 — на `0.99`.

Лейбл `le` необходимо сохранять при агрегации, поскольку он содержит границы histogram buckets. Нельзя получить общий p95 простым усреднением p95 отдельных подов: сначала нужно агрегировать buckets, а затем вычислять квантиль.

Если нужен общий квантиль по всем подам и датацентрам, используйте `sum by (le)` вместо `sum by (le, pod, datacenter)`.

### 2. Throughput: запросы в секунду (RPS)

```promql
sum by (pod, datacenter, uri, method, status) (
  rate(http_requests_total{
    datacenter=~"$datacenter",
    pod=~"$pod"
  }[5m])
)
```

`rate()` вычисляет среднюю скорость прироста счётчика за последние пять минут. `sum` агрегирует результаты по указанным лейблам.

Чтобы отобразить RPS конкретной ручки, добавьте фильтр по `uri` и `method`, например:

```promql
sum by (pod, datacenter) (
  rate(http_requests_total{
    uri="/items/{id}",
    method="GET",
    datacenter=~"$datacenter",
    pod=~"$pod"
  }[5m])
)
```

### 3. Ошибки по HTTP-кодам

```promql
sum by (pod, datacenter, uri, method, status) (
  rate(http_errors_total{
    datacenter=~"$datacenter",
    pod=~"$pod"
  }[5m])
)
```

Метрика `http_errors_total` учитывает ответы с HTTP-кодами `4xx` и `5xx`. Запрос позволяет анализировать скорость возникновения ошибок с разбивкой по коду ответа.

Для отображения только серверных ошибок `5xx`:

```promql
sum by (pod, datacenter, uri, status) (
  rate(http_errors_total{
    status=~"5..",
    datacenter=~"$datacenter",
    pod=~"$pod"
  }[5m])
)
```

### 4. Доля ошибок 5xx

```promql
100 *
sum(
  rate(http_errors_total{
    status=~"5..",
    datacenter=~"$datacenter",
    pod=~"$pod"
  }[5m])
)
/
clamp_min(
  sum(
    rate(http_requests_total{
      datacenter=~"$datacenter",
      pod=~"$pod"
    }[5m])
  ),
  1e-9
)
```

Результат выражается в процентах и показывает долю ответов `5xx` среди всех HTTP-запросов за выбранное временное окно.

`clamp_min()` предотвращает деление на ноль при отсутствии трафика.

### 5. Общий RPS по всем экземплярам

```promql
sum(
  rate(http_requests_total{
    datacenter=~"$datacenter",
    pod=~"$pod"
  }[5m])
)
```

Запрос возвращает суммарное количество запросов в секунду с учётом выбранных фильтров датацентра и пода.

### Часть 3 — нагрузочный инструмент

- [+] Инструмент/скрипт фоновой нагрузки 20–50 RPS на обе ручки-экземпляра, нарастание в течение нескольких минут, ошибки 404 и 500 с настраиваемой вероятностью — TODO
- [+] Раскомментировать сервис `load-generator` в docker-compose.yaml (depends_on: `dashboard-generator` c `condition: service_completed_successfully`) — TODO

### Сдача

- [ ] Инициализировать git-репозиторий и запушить в публичный GitHub
- [ ] Скринкаст (1 минута) под нагрузкой и PR в https://github.com/itmo-ipkn/highload_autumn_2026
