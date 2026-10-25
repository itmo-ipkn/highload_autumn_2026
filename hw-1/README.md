# TASK1 — автоматизация мониторинга в Grafana

Учебный pet-project для DDIA, DevOps & Fullstack, осень 2026. Два экземпляра
FastAPI-приложения экспортируют золотые сигналы REST-ручек. Prometheus собирает
метрики, CLI получает актуальный список ручек и создаёт дашборд в Grafana,
а отдельный процесс постепенно поднимает фоновую нагрузку.

Демонстрация: https://files.sberdisk.ru/s/K6SsEJdvRsz9zQb. В записи показан работающий
стенд; условия проверки описаны в [docs/validation.md](docs/validation.md).

## Запуск

Нужны Docker Engine / Docker Desktop с современным Docker Compose v2 и доступные
порты 8001, 8002, 9090 и 3000. Из корня репозитория:

```bash
cp .env.example .env
docker compose up
```

Дополнительные настройки в интерфейсе Grafana не нужны. При первом запуске
скачиваются образы и собирается Python-приложение. После готовности сервисов
начинается нагрузка; в течение 2–3 минут графики заполняются, а нагрузка выходит
на 36 RPS суммарно. Генератор дашборда штатно завершается с кодом `0`.

| Адрес | Назначение |
|---|---|
| http://localhost:3000/d/items-golden-signals | Готовый дашборд |
| http://localhost:3000 | Grafana; логин и пароль из `.env` |
| http://localhost:9090/targets | Два Prometheus target в состоянии UP |
| http://localhost:8001/health | Под `1`, датацентр `dc-a` |
| http://localhost:8002/health | Под `2`, датацентр `dc-b` |
| http://localhost:8001/handlers | Runtime-каталог бизнес-ручек |
| http://localhost:8001/metrics | Метрики первого экземпляра |
| http://localhost:8002/metrics | Метрики второго экземпляра |

`.env.example` содержит публичные демонстрационные значения
`admin` / `local-demo-change-me`. Собственные значения задаются в локальном `.env`.
Он исключён из Git и Docker build context. Все внешние порты привязаны к
`127.0.0.1`. В коде генератора нет пароля: учётные данные поступают через окружение.

```bash
docker compose ps -a
docker compose logs -f dashboard-generator load-generator
```

Остановка с сохранением томов Prometheus и Grafana:

```bash
docker compose down
```

Записи приложения хранятся только в памяти и теряются при перезапуске процесса.
Пароль Grafana из окружения используется при первоначальном создании администратора;
изменение `.env` не сбрасывает пароль в уже существующем томе Grafana.

## Компоненты и порядок запуска

| Сервис Compose | Назначение и зависимости |
|---|---|
| `app1`, `app2` | Один образ приложения, разные `POD_ID` и `DATACENTER`; readiness через `/health` |
| `prometheus` | Скрейпинг обоих `/metrics` каждые 5 секунд; ждёт здоровые приложения |
| `grafana` | Автоматический provisioning datasource; ждёт здоровый Prometheus |
| `dashboard-generator` | Ждёт здоровые приложения и Grafana; получает `/handlers` и создаёт дашборд |
| `load-generator` | Ждёт здоровые приложения и `service_completed_successfully` генератора |

В Compose используются именно `condition: service_healthy` и
`condition: service_completed_successfully`. Генератор возвращает ненулевой код,
если получение каталога, проверка datasource или создание дашборда не удались.
Поэтому ошибка генерации не считается успешным завершением зависимости.

Все Python-сервисы используют один образ `grafana-task1-app:1.0.0`. Приложение
работает от непривилегированного пользователя с одним Uvicorn worker: каждый
процесс имеет собственное in-memory хранилище. `pull_policy: build` позволяет
пересобирать изменённый код при обычном `docker compose up`.

Основные закреплённые версии:

| Компонент | Версия |
|---|---|
| Python Docker image | `3.12.15-slim-bookworm` |
| FastAPI | `0.162.0` |
| Uvicorn | `0.49.0` |
| Prometheus Python client | `0.26.0` |
| HTTPX для нагрузки | `0.28.1` |
| Prometheus | `v3.15.0` |
| Grafana OSS | `13.2.4` |

Полные наборы зависимостей закреплены в `requirements.txt` и `requirements-dev.txt`.
Генератор собирает JSON напрямую, без SDK. Используемый `POST /api/dashboards/db`
поддерживается закреплённой версией Grafana; этот legacy API объявлен устаревающим,
поэтому при обновлении Grafana следует повторить интеграционную проверку.

## Контракт API

| Метод и путь | Успешный ответ | Отсутствующая запись |
|---|---|---|
| `POST /items` | `201`, созданная запись с UUID | Не применимо |
| `GET /items/{id}` | `200`, запись | `404` |
| `PUT /items/{id}` | `200`, заменённая запись с прежним ID | `404`, без upsert |

Тело POST / PUT:

```json
{"name": "example", "description": "example item"}
```

`name` обязателен, длина 1–200 символов. `description` — строка до 2000 символов,
по умолчанию пустая. PUT заменяет представление полностью: пропущенное описание
становится пустым. Лишние поля и невалидные данные дают `422`. Установленный
демонстрационный режим может вернуть `500` для любой бизнес-ручки.

```bash
curl -i -X POST http://localhost:8001/items \
  -H 'Content-Type: application/json' \
  -d '{"name":"example","description":"created manually"}'

# Подставьте ID из ответа того же экземпляра:
curl -i http://localhost:8001/items/ID_ИЗ_ОТВЕТА
curl -i -X PUT http://localhost:8001/items/ID_ИЗ_ОТВЕТА \
  -H 'Content-Type: application/json' \
  -d '{"name":"updated","description":"new value"}'

curl -i http://localhost:8001/items/missing-id
```

У экземпляров независимые словари: ID, созданный на порту 8001, не существует на
8002. Хранилище защищено блокировкой; автоматического удаления старых записей нет.

Служебные GET `/health`, `/metrics`, `/handlers` исключены из бизнес-метрик.
Автоматические `/docs`, `/redoc`, `/openapi.json` отключены; DELETE не добавлен.

## Библиотека мониторинга

`app/telemetry.py` содержит ASGI middleware, отдельный Prometheus registry на
приложение и функцию обнаружения маршрутов. Инструментирование подключается один
раз; в коде middleware нет перечня бизнес-ручек. Каталог строится из реальной
таблицы FastAPI, включая подключённые роутеры и их префиксы.

Middleware перехватывает `http.response.start` и последний `http.response.body`.
Код ответа считывается из ASGI-события, а время измеряется монотонными часами.
Latency заканчивается при отправке последней части тела ответа в ASGI-сервер;
это серверная длительность, а не полный сетевой RTT клиента. Необработанные
исключения до отправки ответа учитываются как `500` и пробрасываются дальше
стандартному обработчику ошибок. Один запрос даёт ровно одно наблюдение.
Для необработанного исключения до начала ответа длительность фиксируется в
момент ошибки; затем внешний обработчик Starlette отправляет ответ 500.

| Имя метрики | Тип | Лейблы | Назначение |
|---|---|---|---|
| `http_request_duration_seconds` | Histogram | `pod_id`, `datacenter`, `method`, `route` | Распределение длительностей в секундах; экспорт `_bucket`, `_sum`, `_count` |
| `http_requests_total` | Counter | `pod_id`, `datacenter`, `method`, `route`, `status_code` | Общее число ответов, RPS и ошибки по конкретным кодам |

`pod_id` и `datacenter` берутся из `POD_ID` и `DATACENTER`. `route` содержит
шаблон, например `/items/{id}`, поэтому новый UUID не создаёт новую временную
серию. Метод — отдельный лейбл: GET и PUT одного шаблона различаются.
Неизвестные URL и неподдерживаемые методы не приписываются существующим ручкам;
404 отсутствующего объекта на `GET /items/{id}` учитывается.

При старте создаются нулевые серии для известных маршрутов и распространённых
статусов, чтобы селекторы видели поды ещё до нагрузки. Нулевой Counter не означает
наличия запросов. Иные реальные коды добавляются при первом наблюдении.

### Почему Histogram

Нужны перцентили объединённых запросов нескольких экземпляров. Готовые квантили
Summary нельзя корректно усреднить: среднее двух p95 не равно p95 общей выборки.
У классической Histogram одинаковые кумулятивные бакеты можно сложить между
экземплярами и только затем вычислить `histogram_quantile`.

Это корректная агрегация распределений, но значение перцентиля остаётся оценкой:
точность ограничена выбранными бакетами и интерполяцией внутри бакета.
Границы одинаковы во всех экземплярах и заданы в секундах:

```text
0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.075,
0.1, 0.15, 0.25, 0.5, 1, 2.5, 5, 10, +Inf
```

Скрейпинг идёт каждые 5 секунд; такое же значение задано у datasource Grafana.
В запросах используется `$__rate_interval`. `rate` применяется к исходным сериям
до суммирования, чтобы отдельно учитывать сбросы счётчиков при перезапуске подов.

### PromQL

Пример p95 для GET. Для p50 / p99 генератор меняет первый аргумент на 0.50 / 0.99:

```promql
histogram_quantile(0.95,
  sum by (le) (
    rate(http_request_duration_seconds_bucket{
      method="GET", route="/items/{id}",
      datacenter=~"${datacenter:regex}", pod_id=~"${pod_id:regex}"
    }[$__rate_interval])
  )
)
```

Суммарный RPS выбранных экземпляров, включая ошибки:

```promql
sum(rate(http_requests_total{
  method="GET", route="/items/{id}",
  datacenter=~"${datacenter:regex}", pod_id=~"${pod_id:regex}"
}[$__rate_interval]))
```

Ошибки в секунду с отдельной линией для каждого кода:

```promql
sum by (status_code) (rate(http_requests_total{
  method="GET", route="/items/{id}",
  datacenter=~"${datacenter:regex}", pod_id=~"${pod_id:regex}",
  status_code=~"4..|5.."
}[$__rate_interval]))
```

Counter сохраняет количество ответов; на временных графиках показывается их
скорость. Панели ошибок используют единицы операций в секунду, не проценты.

## Генератор дашборда

`GET /handlers` возвращает каталог вида:

```json
{
  "service": "items-api",
  "handlers": [
    {"method": "POST", "path": "/items"},
    {"method": "GET", "path": "/items/{id}"},
    {"method": "PUT", "path": "/items/{id}"}
  ]
}
```

`tools/dashboard_generator.py` получает этот JSON в рантайме, проверяет формат и
строит три панели на каждую пару метод/путь: latency с тремя перцентилями, RPS,
ошибки по кодам. Для исходного приложения получается девять панелей.

В datasource provisioning закреплён UID `task1-prometheus`; UID дашборда —
`items-golden-signals`. Генератор проверяет наличие datasource, сохраняет JSON
с `overwrite: true`, затем проверяет сохранённый дашборд. Повторный запуск
обновляет ту же запись. Учётные данные читаются только из окружения и не выводятся
в логах; HTTP-запросы имеют таймаут и ограниченное число повторов.

Селекторы `datacenter` и `pod_id` имеют `multi: true`, `includeAll: true`,
`allValue: ".*"`; исходно выбран All. Список подов зависит от выбранного датацентра.
Для множественного выбора используется regex-сопоставление. У самого дашборда
задано `editable: false`.

Повторная генерация уже запущенного стека:

```bash
docker compose run --rm --no-deps dashboard-generator
```

Посмотреть JSON без записи в Grafana:

```bash
docker compose run --rm --no-deps dashboard-generator \
  python -m tools.dashboard_generator --dry-run
```

Если добавить бизнес-ручку в приложение, достаточно пересобрать и перезапустить
стек. `/handlers` и middleware обнаружат маршрут, а генератор создаст новые панели
без изменения своего кода. Это поведение отдельно покрыто тестами.

## Нагрузка и демонстрационные ошибки

`tools/load_generator.py` использует `asyncio` и HTTPX с постоянными соединениями.
Он реализует общее расписание на оба экземпляра: линейный рост с 3 до 36 RPS
за 120 секунд, затем постоянную нагрузку до SIGTERM / Ctrl+C.

Запросы распределяются между подами по очереди; обычный цикл методов — 20% POST,
40% GET, 40% PUT. У каждого адреса свой ограниченный пул ID. Перед ramp-up
создаётся начальная запись на каждом экземпляре. Случайный переход с ID одного
пода на другой исключён.

3% всех отправляемых запросов планируются как GET / PUT отсутствующего ID.
Приложение независимо возвращает искусственный `500` с вероятностью 2% на
бизнес-запрос. Если сбой сработал на запланированном отсутствующем ID, наблюдается
500 вместо 404. Поэтому ожидаемая общая доля ошибок около 4.94%, а фактические
значения немного меняются из-за случайности и окна усреднения.

Для наглядной latency приложение добавляет асинхронную задержку 5–120 мс.
Задержки и искусственные сбои отключаются нулевыми настройками; служебные ручки
от них не зависят. Логика демонстрационных сбоев находится в `app/demo.py`,
отдельно от библиотеки мониторинга.

Нагрузка запускает запросы по времени, не ожидая завершения предыдущего запроса.
Одновременно разрешено до 64 запросов. При исчерпании лимита или сильном отставании
пропускаются слоты; не накапливаются бесконечная очередь и догоняющий всплеск.
Раз в 10 секунд выводится JSON: предложенный/отправленный/завершённый RPS,
HTTP-коды, сетевые ошибки, пропуски и статистика по каждому адресу.

Основные настройки `.env`:

| Переменная | По умолчанию | Значение |
|---|---|---|
| `FAILURE_RATE` | `0.02` | Вероятность HTTP 500 на бизнес-запрос |
| `LATENCY_MIN_MS` / `LATENCY_MAX_MS` | `5` / `120` | Диапазон демонстрационной задержки |
| `LOAD_START_RPS` / `LOAD_TARGET_RPS` | `3` / `36` | Начальная и целевая суммарные скорости |
| `LOAD_RAMP_SECONDS` | `120` | Продолжительность плавного разгона |
| `LOAD_MISSING_RATE` | `0.03` | Доля запланированных запросов к отсутствующим ID |
| `LOAD_MAX_IN_FLIGHT` | `64` | Лимит одновременных запросов |

## Проверки

Локальные тесты на Python 3.12:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements-dev.txt
python -m unittest discover -v
```

Тесты проверяют CRUD, конкурентные обращения, лейблы и коды метрик, исключения,
служебные пути, новые маршруты и префиксы роутеров, совместимость бакетов,
динамическую генерацию, формулы и переменные дашборда, ошибки Grafana API,
распределение нагрузки и ограничение concurrency.

Проверка конфигураций:

```bash
docker compose --env-file .env.example config --quiet
docker compose run --rm --no-deps --entrypoint /bin/promtool \
  prometheus check config /etc/prometheus/prometheus.yml
```

Проверка работающего стека после запуска:

```bash
python3 scripts/check_stack.py --wait 180
```

Скрипт использует стандартную библиотеку Python, читает учётные данные из `.env`,
проверяет обе реплики, Targets, datasource, дашборд, селекторы, фактические PromQL,
покрытие всех ручек на обоих подах, суммарные 20–50 RPS и минимум два кода ошибок.

Автоматический workflow `.github/workflows/ci.yml` запускает тесты и проверку
Docker Compose на GitHub Actions. Фактически выполненные при подготовке проверки
и ограничения среды перечислены в [docs/validation.md](docs/validation.md).

## Файлы проекта

| Путь | Содержание |
|---|---|
| `app/main.py`, `app/models.py`, `app/store.py` | CRUD, контракт и in-memory данные |
| `app/config.py`, `app/demo.py` | Настройки экземпляра, задержки и искусственные 500 |
| `app/telemetry.py` | Библиотека метрик и runtime-каталог |
| `tools/dashboard_generator.py` | CLI генерации и загрузки дашборда |
| `tools/load_generator.py` | Долговременная нагрузка |
| `infra/` | Prometheus и provisioning Grafana |
| `tests/` | Автоматические тесты |
| `scripts/check_stack.py` | Проверка запущенной системы |
| `docs/validation.md` | Результаты выполненных проверок |
| `docs/submission.md` | Порядок публикации и сдачи |
| `docs/PR_BODY.md` | Подготовленная структура PR с полями автора |
| `docs/demo.mp4`, `docs/demo.srt` | Минутный скринкаст и русские субтитры |

## Первичные источники

- [Prometheus: Histograms and summaries](https://prometheus.io/docs/practices/histograms/).
- [Официальный Python client: Histogram](https://prometheus.github.io/client_python/instrumenting/histogram/).
- [Starlette: ASGI middleware](https://www.starlette.io/middleware/).
- [Grafana: Prometheus variables, Multi-value и All](https://grafana.com/docs/grafana/latest/datasources/prometheus/template-variables/).
- [Grafana: datasource provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/).
- [Grafana: поддержка legacy HTTP API](https://grafana.com/docs/grafana/latest/developer-resources/api-reference/http-api/api-legacy/).
- [Docker Compose: порядок запуска](https://docs.docker.com/compose/how-tos/startup-order/).

Проект подготовлен с использованием AI. Дедлайн из условия: **30 октября 2026 года,
23:59 UTC+3**. Для сдачи необходимо указать автора, группу и свой публичный репозиторий.
