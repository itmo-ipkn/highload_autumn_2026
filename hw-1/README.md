# HW-1. Инструменты автоматизации в Grafana

CRUD-сервис `items` на Go с метриками золотых сигналов, генератор дашборда Grafana по списку
ручек сервиса и фоновая нагрузка на k6.

## Запуск

```bash
cd hw-1
cp .env.example .env
docker compose up
```

Через 2-3 минуты нагрузка выходит на полку, дашборд - <http://localhost:3000> (он домашний).
Смотреть под `GRAFANA_VIEWER_USER` из `.env`: у этого пользователя роль Viewer, дашборд только
на просмотр.

| Сервис | Порт | Что это |
| --- | --- | --- |
| `app-1` | 8081 | приложение, `POD_ID=pod-1`, `DATACENTER=dc-a` |
| `app-2` | 8082 | приложение, `POD_ID=pod-2`, `DATACENTER=dc-b` |
| `prometheus` | 9090 | скрейпит оба экземпляра раз в 5 с |
| `grafana` | 3000 | datasource через provisioning, `uid: prometheus` |
| `dashgen` | - | генерирует дашборд и завершается |
| `load` | - | k6, фоновая нагрузка |

Порядок старта: `app-1`, `app-2` -> `prometheus` -> `grafana` -> `dashgen` (все `service_healthy`)
-> `load` (`service_completed_successfully` у `dashgen`).

## Приложение

| Ручка | Ответ |
| --- | --- |
| `POST /items` | 201, 400 на невалидное тело |
| `GET /items/{id}` | 200, 404 если нет |
| `PUT /items/{id}` | 200, 404 если нет |
| `GET /health` | 200 |
| `GET /metrics` | метрики Prometheus |
| `GET /handlers` | `{"handlers":[{"method":"GET","route":"/items/{id}"}, ...]}` |

Хранилище in-memory: `map` под `sync.RWMutex`.

| Переменная | По умолчанию | Что делает |
| --- | --- | --- |
| `POD_ID`, `DATACENTER` | `local` | лейблы `pod` и `datacenter` |
| `FAIL_RATE` | `0`, в compose `0.02` | доля искусственных 500 |
| `MAX_DELAY` | `0s`, в compose `50ms` | случайная задержка до этого значения |
| `LISTEN_ADDR` | `:8080` | адрес сервера |

## Метрики

Middleware на роутере chi (`internal/metrics`). Бизнес-ручки висят на отдельном роутере с
middleware, служебные `/health`, `/metrics`, `/handlers` - на корневом и в метрики не попадают.

| Метрика | Тип | Лейблы |
| --- | --- | --- |
| `http_server_request_duration_seconds` | histogram | `method`, `route`, `code`, `pod`, `datacenter` |
| `http_server_requests_total` | counter | `method`, `route`, `code`, `pod`, `datacenter` |

* `route` - шаблон пути chi (`/items/{id}`), а не фактический путь: число серий не зависит от id.
  Пути, которые не сматчились ни с одной ручкой, не пишутся.
* `pod`, `datacenter` - const labels из env, навешиваются на все серии экземпляра.
* Ошибки - срез `http_server_requests_total` по `code=~"4..|5.."`: RPS и ошибки из одного
  источника, без отдельной метрики.
* Бакеты `1ms ... 1s` под in-memory ручки с задержкой до 50 ms.

**Почему histogram.** Summary считает квантили внутри процесса, а квантили разных подов
нельзя ни сложить, ни усреднить. Бакеты histogram - обычные счётчики: их можно просуммировать
по подам и датацентрам и уже по сумме посчитать `histogram_quantile`. Перцентиль получается
корректным для любого набора экземпляров. Цена - точность в пределах бакета.

`/handlers` строится обходом роутера (`chi.Walk`): новая ручка попадает в список без правок.

## Дашборд

`dashgen` (`cmd/dashgen`, `internal/dashboard`) - CLI на Go без SDK:

1. забирает `/handlers` у всех экземпляров из `APP_URLS` и объединяет;
2. ждёт `/api/health` Grafana;
3. собирает JSON дашборда из своих структур;
4. `POST /api/dashboards/db` с `overwrite: true` и фиксированным `uid`, учётка админа из env.

На каждую ручку - строка с тремя панелями. Матчеры ручки и селекторов:
`method="GET", route="/items/{id}", datacenter=~"$datacenter", pod=~"$pod"`.

| Панель | PromQL |
| --- | --- |
| Latency | `histogram_quantile(0.5\|0.95\|0.99, sum by (le) (rate(http_server_request_duration_seconds_bucket{...}[$__rate_interval])))` |
| RPS | `sum(rate(http_server_requests_total{...}[$__rate_interval]))` |
| Ошибки | `sum by (code) (rate(http_server_requests_total{..., code=~"4..\|5.."}[$__rate_interval]))` |

Селекторы `datacenter` и `pod` - multi-value с All, `pod` фильтруется по выбранным датацентрам.

Дашборд нельзя править:

* `editable: false` в JSON;
* `dashgen` заводит пользователя `GRAFANA_VIEWER_USER` с ролью Viewer: у него нет кнопок
  Edit и Make editable, API отвечает `canEdit: false`. Админ в Grafana может всё, поэтому
  дашборд смотрим под viewer;
* дашборд домашний для организации: после входа открывается сразу он.

Список ручек в генераторе не хранится: добавить ручку в `api.Routes`, перезапустить стек -
появится новая строка.

## Нагрузка

k6, `ramping-arrival-rate` (`load/load.js`): за `LOAD_RAMP` (3 мин) от 1 до `LOAD_RATE`
(30) запросов в секунду, дальше полка до остановки стека. Одна итерация - один запрос:

* экземпляр выбирается случайно;
* 30 % `POST /items`, 50 % `GET /items/{id}`, 20 % `PUT /items/{id}`;
* 5 % GET и PUT идут к несуществующим id -> 404, плюс 500 от `FAIL_RATE`.

Итого около 5 % ошибок двух кодов.

## Секреты

Логины и пароли админа и viewer в Grafana только в `.env`, он в `.gitignore`. В репозитории
`.env.example`.
Без `.env` compose падает с ошибкой.

## CI

`.github/workflows/hw-1.yml` на каждый PR:

* `go` - gofmt, go vet, `go test -race`, golangci-lint;
* `docker` - hadolint, `promtool check config`, весь стек через `docker compose up`, затем
  проверки: ручки и метрики обоих экземпляров, таргеты Prometheus, datasource и дашборд в
  Grafana (non-editable, по 3 панели на каждую ручку из `/handlers`, у viewer нет прав на
  правку), нагрузка доходит до
  всех ручек и подов, видны 404 и 500;
* `actionlint`.
