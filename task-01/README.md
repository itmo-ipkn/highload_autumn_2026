# TASK1. Инструменты автоматизации в Grafana 

CRUD-сервис на node.js/express с метриками золотых сигналов (`rps`, `latency`, `errors`), динамический генератор дашбордов `Grafana` по динамическому списку эндпойнтов сервиса и фоновая нагрузка на `k6`.

## Быстрый старт
```bash
cd task-01

cp .env.example .env

docker compose up --build
```

### Ссылки
- Grafana: http://localhost:3000/
- Prometheus: http://localhost:9090/
- Пример запроса в API: `curl http://localhost:{PORT}/handlers` 
- Скринкаст: https://drive.google.com/file/d/1-ukwABAMJdFVIeW_erHzBumUEwepDVxP/view?usp=sharing

### Endpoints

| Method | Endpoint
| :--- | :--- | 
| POST | /items | 
| GET | /items/{id} | 
| PUT | /items/{id} | 
| GET | /health | 
| GET | /metrics | 
| GET | /handlers | 

### Метрики
`http_requests_total`, `http_request_duration_seconds`, `http_errors_total`


### Лейблы для метрик:

* datacenter: идентификатор датацентра (dc-001, dc-002 и т.д.)
* pod_id: идентификатор пода/экземпляра (pod-001, pod-002)
* method: HTTP-метод запроса
* endpoint: путь запроса
* status_code: код ответа

### Тип метрики для Latency
Для измерения Latency был выбран тип метрики Histogram

Причины выбора:

* Расчёт перцентилей: Histogram позволяет корректно вычислять перцентили (p50, p95, p99) даже при агрегации данных с нескольких экземпляров. Формула histogram_quantile в PromQL даёт статистически верный результат.
* Защита от выбросов: среднее арифметическое (Average) сильно искажается редкими, но очень долгими запросами. Перцентили показывают реальную картину: «95% пользователей получили ответ быстрее X мс».
* Гибкость анализа: благодаря заранее заданным «вёдрам» (buckets) мы можем анализировать распределение задержек в нужных диапазонах (например, отличать нормальные ответы от таймаутов).