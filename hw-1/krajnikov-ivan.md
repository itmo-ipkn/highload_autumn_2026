# HW-1 — Крайников Иван Александрович

- Трек: DevOps
- Группа: M4226
- Скринкаст: https://disk.yandex.ru/i/AuKbdy-6Xs0myA
- Репозиторий: https://github.com/cry1s/mag3-highload-lab1

## Что внутри

- Go-сервис: `POST /items`, `GET /items/{id}`, `PUT /items/{id}`, in-memory хранилище, `GET /health`.
- Библиотека инструментирования: histogram `http_request_duration_seconds` (p50/p95/p99,
  корректно агрегируется по подам через `sum by (le)`) и counter `http_requests_total`
  (RPS и ошибки по кодам ответа). Лейблы `pod_id`, `datacenter`, `handler`, `method`, `status_code`.
- Каталог ручек в `GET /handlers` и CLI-генератор дашборда Grafana, который читает его в рантайме.
- Нагрузочный генератор: ramp-up до 30 RPS, оба экземпляра, 4xx/5xx в трафике.
- `docker compose up` поднимает два экземпляра сервиса, Prometheus, Grafana, генератор дашборда и нагрузку.