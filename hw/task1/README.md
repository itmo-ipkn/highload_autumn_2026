
## Запуск

```sh
cd hw/task1/
cp .env.example .env
docker compose up
```

## Приложение
| Сервис | Где работает | Порты (наружу) | Что можно просмотреть | Зависит от |
|---|---|---|---|---|
| **api-dc1** | контейнер `api-dc1`, сеть `backend`, ДЦ `dc-1` | внутренний `8000` | `http://api-dc1:8000/health`; API изнутри сети | — |
| **api-dc2** | контейнер `api-dc2`, сеть `backend`, ДЦ `dc-2` | внутренний `8000` | `http://api-dc2:8000/health`; API изнутри сети | — |
| **prometheus** | контейнер `prometheus`, сеть `backend` | **`9090`** | UI: `http://localhost:9090` — targets, метрики, графики; сбор с api-dc1/api-dc2 | api-dc1, api-dc2 |
| **grafana** | контейнер `grafana`, сеть `backend` | **`3000`** | UI: `http://localhost:3000` — дашборды, графики; анонимно `Viewer` | — |
| **dashboard-generator** | одноразовый контейнер, сеть `backend` | нет | Дашборды в Grafana; логи контейнера | api-dc1, api-dc2, grafana — healthy |
| **load-generator** | контейнер (`restart: unless-stopped`), сеть `backend` | нет | Нагрузка (20 воркеров) в метриках Prometheus и дашбордах Grafana; логи | dashboard-generator — завершён; api-dc1, api-dc2, grafana — healthy |

- **[Демонстрация работы](demo.mp4)**

## Сбор метрик
```python
selector = f'handler="{path}", method="{method}", datacenter=~"$datacenter", pod_id=~"$pod_id"'
```
Общий селектор
### Latency (перцентили 50, 95 и 99)
Пример для p50
```python
f"histogram_quantile(0.50, sum(rate(http_request_duration_seconds_bucket{{{selector}}}[$__rate_interval])) by (le))"
```
`http_request_duration_seconds_bucket` накопления счётчиков по корзинам

`rate+sum by (le)` дают скорость прироста по каждой корзине

`histogram_quantile` считает внутри корзины по заданному параметру (перцентили)

### RPS
```python
f"sum(rate(http_requests_total{{{selector}}}[$__rate_interval]))"
```
`http_requests_total` все запросы по селектору

`rate` скорость прироста

`sum` считает общее кол-во

### Ошибки по кодам ответа
```python
f'sum(rate(http_requests_total{{{selector}, status=~"4..|5.."}}[$__rate_interval])) by (status)'
```
Так же как и `RPS`, но с группировкой по статусу

- **[Построение дашбордов](backend/gen_dashboard.py)**
- **[Генерация нагрузки](backend/load_test.py)**
