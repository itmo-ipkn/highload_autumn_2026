# Golden Signals: мониторинг микросервиса (HW-1)

Pet-проект: CRUD-микросервис на Flask с метриками Prometheus (latency, traffic,
errors — три золотых сигнала), автоматическая генерация дашборда Grafana и
нагрузочный генератор. Всё поднимается одной командой `docker compose up`.

## Структура

    app/                  — микросервис (Flask + prometheus-client)
    monitoring/           — конфиг Prometheus и provisioning Grafana
    scripts/              — генератор дашборда и нагрузочный генератор
    docker-compose.yml    — 6 сервисов: app1, app2, prometheus, grafana,
                            dashboard-generator, load-generator

## Запуск

    cp .env.example .env
    docker compose up --build -d

Через 2–3 минуты после запуска всё готово к просмотру: дашборд создаётся
генератором автоматически, нагрузка ramps up за `RAMP_UP_SECONDS` секунд.

## Порты

| Сервис      | Порт (host) | Назначение                     |
|-------------|-------------|--------------------------------|
| app1        | 8081        | экземпляр сервиса, POD_ID=1, dc1 |
| app2        | 8082        | экземпляр сервиса, POD_ID=2, dc2 |
| Prometheus  | 9090        | UI и API метрик                |
| Grafana     | 3000        | дашборды (admin / пароль из .env) |

## Проверка

1. Health-checks:
   curl http://localhost:8081/health
   curl http://localhost:8082/health
2. Микросервис: POST http://localhost:8081/items (json: {"name": "x"}),
   GET http://localhost:8081/items/1, PUT http://localhost:8081/items/1.
3. Список ручек: curl http://localhost:8081/handlers
4. Метрики: curl http://localhost:8081/metrics
5. Grafana: http://localhost:3000 → логин admin, пароль из .env → дашборд
   «Items service — Golden Signals». Селекторы Датацентр/Под — multi + All.
6. Нагрузка: контейнер load-generator виден в `docker compose ps` и пишет
   статистику в `docker compose logs -f load-generator`.

## Метрики и лейблы

- `http_requests_total{pod_id, datacenter, handler, method, code}` — Counter:
  throughput (RPS через `rate`) и errors (разбивка по конкретным кодам 4xx/5xx).
- `http_request_duration_seconds{pod_id, datacenter, handler, method}` —
  Histogram с бакетами. Выбор типа для latency: только Histogram позволяет
  корректно считать перцентили при агрегации по нескольким экземплярам —
  `histogram_quantile(q, sum by (le) (rate(..._bucket[...])))`. Gauntlet из
  Summary не подходит: quantile нельзя суммировать по инстансам, квантили
  посчитанные на каждом поде по отдельности, усреднять нельзя.
- `handler` — шаблон пути (`/items/{id}`), а не фактический URL запроса;
  `pod_id` и `datacenter` пробрасываются через env-переменные POD_ID/DATACENTER.

## Дашборд

Генератор дашборда (scripts/dashboard_generator.py) запускается docker-compose
после health-проверок app1/app2 и Grafana. Он динамически запрашивает список
ручек у обоих экземпляров (GET /handlers) и на каждую ручку создаёт строку с
панелями: latency p50/p95/p99, RPS, ошибки по кодам. Список ручек в скрипте
не захардкожен: добавление новой ручки в приложение отразится на дашборде
после перезапуска стека без изменений кода генератора. Дашборд non-editable,
datasource Prometheus создаётся provisioning-файлом с фиксированным uid
`prometheus`.

## Нагрузка

Скрипт ramps up нагрузку до TARGET_RPS (по умолчанию 30 суммарно на оба
экземпляра) за RAMP_UP_SECONDS (120 с) и держит её бесконечно. Запросы
равномерно распределяются между подами, затрагиваются все ручки. Ошибки
создаются двумя способами: 10% GET идут на несуществующий id (404) и около
FAIL_RATE (5%) всех запросов падают 500 из-за искусственных сбоев в сервисе.

Примечание: вместо requests-html используется requests + пул потоков —
requests-html требует браузерный движок pyppeteer и не обеспечивает
стабильный RPS; по условию задание language-agnostic и допускает любой
инструмент нагрузки.

## Безопасность

Пароль администратора Grafana задаётся только в .env (файл в .gitignore),
в репозитории лежит .env.example с примером значения.

## Остановка

    docker compose down

## Ссылка на скринкаст

https://disk.yandex.ru/i/6fqSfY7z5gj7Sg
