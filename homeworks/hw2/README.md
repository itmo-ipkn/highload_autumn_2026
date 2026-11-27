# HW2: Kafka producer и consumer

## Запуск

```bash
cp .env.example .env
docker compose up -d --build
```

Продюсеры и консьюмеры запускаются после успешного завершения `kafka-init`; консьюмеры дополнительно ждут готовности Postgres. Каждый продюсер публикует события в `events` со скоростью `RATE_PER_SEC` событий в секунду. По умолчанию суммарная скорость трёх экземпляров — 300 событий/с. Метрики каждого продюсера доступны внутри compose-сети на порту `2112`.

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
- Настройки консьюмера задаются через `POD_ID`, `KAFKA_BROKERS`, `KAFKA_TOPIC`, `KAFKA_CONSUMER_GROUP`, `KAFKA_VERSION`, `POSTGRES_URL`, `PROCESSING_DELAY` и `COMMIT_INTERVAL`.

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

Для консьюмера используются те же цели, а также `make build-verify` и `make run-verify` после реализации соответствующих исходников.
