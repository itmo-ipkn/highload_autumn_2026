# HW2: Kafka producer и consumer

## Запуск

```bash
cp .env.example .env
docker compose up -d --build
```

Продюсеры и консьюмеры запускаются после успешного завершения `kafka-init`; консьюмеры дополнительно ждут готовности Postgres. Каждый продюсер публикует события в `events` со скоростью `RATE_PER_SEC` событий в секунду. По умолчанию суммарная скорость трёх экземпляров — 300 событий/с. Метрики продюсеров доступны внутри compose-сети на порту `2112`, метрики консьюмеров — на `2113`.

Grafana: http://localhost:3000 (анонимный доступ Viewer; Prometheus: http://localhost:9090). В обоих сервисах адрес привязывается к `127.0.0.1` по умолчанию. Пароли Grafana и Postgres не заданы в compose: если `GRAFANA_ADMIN_PASSWORD` или `POSTGRES_PASSWORD` не заполнены в `.env`, `secrets-init` генерирует их и сохраняет в приватном Docker volume `credentials`. Имя администратора можно задать через `GRAFANA_ADMIN_USER`. Дашборд `Kafka HW2 — producer & consumer` и Prometheus datasource создаются автоматически из файлов provisioning. Для доступа снаружи localhost отключите анонимный режим и задайте уникальный `GRAFANA_ADMIN_PASSWORD`.

Чтобы получить сгенерированные учётные данные администратора Grafana:

```bash
docker run --rm -v highload-hw2_credentials:/credentials:ro alpine:3.24.3 \\
  sh -c 'printf "Grafana user: %s\\nGrafana password: %s\\nPostgres password: %s\\n" "$(cat /credentials/username)" "$(cat /credentials/password)" "$(cat /credentials/postgres-password)"'
```

Секреты сохраняются вместе с данными. При обновлении старого стенда с уже созданными data volumes укажи в `.env` прежние пароли: инициализатор остановится, если данные есть, а credentials volume отсутствует, чтобы не создать несогласованные учётные данные. Чтобы изменить пароль через `.env`, нужно удалить тома командой `docker compose down -v` и поднять стенд заново; команда удалит и данные Kafka, Postgres, Grafana и Prometheus. Пароль Postgres при ручной настройке должен состоять из URL-безопасных символов: букв, цифр, `.`, `_`, `~` или `-`.

Prometheus скрейпит три экземпляра producer и consumer каждые 5 секунд; `kafka-exporter` отдаёт метрики брокеров и consumer lag. Метрики приложения:

- producer: `producer_events_generated_total`, `producer_messages_enqueued_total`, `producer_messages_acked_total`, `producer_messages_failed_total`, `producer_ack_latency_seconds` (histogram), `producer_in_flight_messages`;
- consumer: `consumer_events_processed_total`, `consumer_duplicate_deliveries_total`, `consumer_events_dlt_total`, `consumer_processing_retries_total`, `consumer_group_setups_total`, `consumer_assigned_partitions`, `consumer_partition_offset`, `consumer_processing_duration_seconds` (histogram).

Во всех метриках приложений есть лейбл `pod`; consumer counters также содержат `topic`, а offset gauge — `topic` и `partition`. Latency экспортируется histogram, поэтому дашборд вычисляет квантили через `histogram_quantile` поверх агрегированных `_bucket` (`sum by (le)`). Histogram, в отличие от summary, сохраняет корректность перцентилей при агрегации нескольких подов.

Остановить стенд без удаления данных:

```bash
docker compose down
```

Проверить состояние и логи:

```bash
docker compose ps -a
docker compose logs -f producer-1 producer-2 producer-3
```

## Продюсер

- `producer-1`, `producer-2` и `producer-3` используют один образ и имеют разные `POD_ID`.
- Формат `user_id` — `p{POD_ID}-{RUN_ID}-u{N}`. Поэтому один пользователь принадлежит ровно одному запуску одного пода.
- `seq` увеличивается под мьютексом пользователя и фиксируется только после принятия события асинхронным продюсером.
- События публикуются через Sarama с `acks=all`, идемпотентностью, `MaxOpenRequests=1` и повторными попытками с backoff.
- При `KEY_MODE=user_id` ключ Kafka равен `user_id`, поэтому все события пользователя попадают в одну партицию. При `KEY_MODE=none` ключ не задаётся — этот режим нужен для эксперимента с нарушением порядка.
- Около `POISON_RATE` событий получают поле `payload.poison=true`.
- При `SIGTERM` генерация останавливается, после чего асинхронный продюсер дожидается подтверждения всех буферизованных сообщений в пределах `SHUTDOWN_TIMEOUT`.

Основные настройки продюсера находятся в `.env.example`: скорость, число пользователей, вероятность poison-событий, режим ключа, лимиты retry/flush и `MAX_EVENTS` для конечного прогона.

## Консьюмер и sink

- `consumer-1`, `consumer-2` и `consumer-3` используют одну группу `events-processor`; Kafka распределяет между участниками партиции топика.
- В каждой партиции сообщения обрабатываются последовательно. Консьюмер проверяет JSON-событие и записывает его в `processed_events`.
- Автокоммит отключён. Оффсет отмечается только после успешной записи в Postgres и коммитится с интервалом `COMMIT_INTERVAL` (по умолчанию 1 с), а при завершении обработки партиции/ребалансе выполняется финальный коммит.
- Повторная доставка безопасна: `event_id` — первичный ключ, `INSERT ... ON CONFLICT DO NOTHING` предотвращает повторную запись; повторы регистрируются в `duplicate_deliveries`.
- Настройки консьюмера задаются через `POD_ID`, `KAFKA_BROKERS`, `KAFKA_TOPIC`, `KAFKA_CONSUMER_GROUP`, `KAFKA_VERSION`, `POSTGRES_URL`, `PROCESSING_DELAY` и `COMMIT_INTERVAL`. Prometheus endpoint доступен на `:2113/metrics`, readiness — `:2113/healthz`.

## Проверки и линтеры

В каждом Go-модуле (`producer` и `consumer`) есть собственный Makefile. Для продюсера:

```bash
cd producer
make lint-install  # один раз установить golangci-lint v1.64.8
make fmt           # golangci-lint run ./... --fix --timeout 2m
make check         # golangci-lint + go vet + go test
make race          # тесты с race detector
make build
```

Для консьюмера используются те же цели, а также `make build-verify` и `make run-verify`.

## Эксперименты с отказами

Все сценарии запускаются из `homeworks/hw2` и самостоятельно делают `make reset`: это останавливает стек и удаляет его volumes, затем создаёт топики заново и поднимает систему. Существующие данные будут удалены. Во время эксперимента можно смотреть панели Grafana и логи. По умолчанию генерация длится 30 секунд; длительность настраивается переменной `RUN_SECONDS`, таймаут ожидания нулевого lag — `WAIT_TIMEOUT_SECONDS`.

```bash
make exp1  # аварийное завершение consumer-1 и возврат в группу
make exp2  # остановка kafka-2 и kafka-3, потеря кворума KRaft
make exp3  # KEY_MODE=none и проверка порядка записей
```

Или запустить сценарии напрямую: `bash scripts/exp1_kill_consumer.sh`, `bash scripts/exp2_brokers.sh`, `bash scripts/exp3_keyless.sh`.

### 1. Kill consumer

После нагрузки `consumer-1` завершается через SIGKILL на 15 секунд, затем запускается повторно. Остальные участники должны продолжить обработку после таймаута сессии и ребаланса. Скрипт останавливает все producer, ждёт нулевого lag и вызывает `verify`. Ожидание: нет пропусков seq и нарушений порядка; возможные повторные доставки безопасно отсекаются по `event_id`.

### 2. Остановка двух брокеров

`kafka-2` и `kafka-3` останавливаются на 20 секунд (изменяется через `BROKER_OUTAGE_SECONDS`). В combined mode это одновременно потеря большинства контроллеров KRaft: публикация может перестать подтверждаться, а producer остаются в retry. После запуска узлов скрипт ждёт их health checks, останавливает генерацию, ждёт нулевого lag и запускает `verify`. Ожидание: после восстановления брокеров все последовательности доезжают, потерь нет. Панели `kafka-exporter` показывают состояние кластера и lag группы.

### 3. Публикация без ключа

Сценарий делает полный reset с `KEY_MODE=none`, чтобы ключевые и бесключевые producer не смешивали записи в одном прогоне. После остановки генерации и опустошения очереди `verify` должен обнаружить нарушения порядка записи для пользователей (ненулевой код выхода здесь ожидаем). Скрипт отдельно считает их SQL-запросом к sink и завершается ошибкой, если нарушений не обнаружено. События распределяются по партициям независимо от `user_id`, поэтому консьюмеры обрабатывают события одного пользователя параллельно; пропусков последовательностей ожидаться не должно. Если на коротком прогоне нарушений не возникло, увеличьте `RUN_SECONDS`.
