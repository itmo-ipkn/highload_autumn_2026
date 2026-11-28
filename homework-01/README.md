# Мониторинг Go-микросервиса: Prometheus + Grafana

CRUD-микросервис на Go с метриками золотых сигналов (latency, traffic, errors), автоматически
генерируемым дашбордом Grafana и генератором нагрузки.

## Запуск

```bash
cp .env.example .env    # при желании поменяйте пароль Grafana и порты
docker compose up
```

Через 2–3 минуты на дашборде http://localhost:3000/d/items-service видна фоновая нагрузка ~30 RPS
и ошибки 404/500.

| Сервис | Адрес | Порт в `.env` |
|---|---|---|
| app-1 (`pod-1`, `dc-a`) | http://localhost:18081 | `APP1_PORT` |
| app-2 (`pod-2`, `dc-b`) | http://localhost:18082 | `APP2_PORT` |
| Prometheus | http://localhost:9090 | `PROMETHEUS_PORT` |
| Grafana | http://localhost:3000 (логин/пароль из `.env`) | `GRAFANA_PORT` |

Остановка: `docker compose down`.

Логин и пароль администратора Grafana хранятся только в `.env` (он в `.gitignore`),
в репозитории — `.env.example`.

## Как это устроено

```
loadgen ──HTTP──▶ app-1 ─┐
        ──HTTP──▶ app-2 ─┴── /metrics ──▶ Prometheus ◀── Grafana
                    ▲                                     ▲
                    └── GET /handlers ── dashgen ─────────┘
```

- **app-1, app-2** — два экземпляра одного образа с разными `POD_ID` и `DATACENTER`.
- **Prometheus** (`v3.13.5`) собирает метрики с обоих экземпляров.
- **Grafana** (`13.2.5`) получает datasource Prometheus через provisioning с фиксированным `uid`.
- **dashgen** — запускается, когда приложения и Grafana здоровы, берёт список ручек из `GET /handlers`
  и создаёт дашборд через API Grafana. Ручки в коде генератора не перечислены: новая ручка появится
  на дашборде после перезапуска стека.
- **loadgen** — запускается после `dashgen`, за 2 минуты плавно поднимает нагрузку до ~30 RPS на оба
  экземпляра и держит её. 404 — запросы к несуществующим id, 500 — искусственные сбои в приложении
  (`APP_FAILURE_RATE` в `.env`).

## Микросервис

| Ручка | Описание |
|---|---|
| `POST /items` | создать запись |
| `GET /items/{id}` | получить запись (404, если нет) |
| `PUT /items/{id}` | обновить запись |
| `GET /health` | 200, когда сервис готов |
| `GET /metrics` | метрики в формате Prometheus |
| `GET /handlers` | список ручек: `{"handlers":[{"method":"GET","route":"/items/{id}"}, ...]}` |

Пример: `curl -X POST localhost:18081/items -d '{"name":"book","value":"42"}'`.

## Метрики

Метрики собираются middleware'ом, через который регистрируются бизнес-ручки
(служебные `/health`, `/metrics`, `/handlers` не учитываются).

| Метрика | Тип | Лейблы | Сигнал |
|---|---|---|---|
| `http_request_duration_seconds` | Histogram | `method`, `route`, `pod`, `datacenter` | Latency |
| `http_requests_total` | Counter | `method`, `route`, `code`, `pod`, `datacenter` | Traffic и Errors |

- `method` + `route` — идентификатор ручки; `route` — шаблон пути (`/items/{id}`), а не фактический путь,
  иначе каждый id создавал бы новый временной ряд;
- `code` — HTTP-код ответа; ошибки — это `http_requests_total` с `code=~"4..|5.."`;
- `pod`, `datacenter` — из переменных окружения `POD_ID` и `DATACENTER`.

### Почему Histogram

Summary считает перцентили внутри каждого процесса, и их нельзя корректно объединить:
среднее p99 двух подов — это не p99 общего трафика.

Histogram отдаёт счётчики попаданий в бакеты, а счётчики можно складывать. Поэтому перцентиль по
любому набору подов и датацентров считается честно — сначала суммируем бакеты, потом берём квантиль:

```promql
histogram_quantile(0.99, sum by (le) (rate(http_request_duration_seconds_bucket[1m])))
```

## Дашборд

- Для каждой ручки — строка с тремя панелями: Latency (p50/p95/p99), RPS, Errors by code.
- Селекторы Datacenter и Pod с множественным выбором и All.
- RPS и ошибки — `sum(rate(...))`, перцентили — `histogram_quantile` поверх `sum by (le)`.
- Дашборд нельзя редактировать (`editable: false`).

## Структура

```
cmd/app/            микросервис
cmd/dashgen/        генератор дашборда
cmd/loadgen/        генератор нагрузки
internal/           хранилище, хендлеры, метрики, построение дашборда
deploy/             конфиги Prometheus и provisioning Grafana
```

Тесты: `go test ./...`
