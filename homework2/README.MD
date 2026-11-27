# Домашняя работа 2/3. Kafka

## Запуск инфраструктуры

Из каталога `homework2`:

```sh
docker compose up -d
docker compose ps
docker compose logs kafka-init
```

Дождитесь строки `Kafka topics are ready.` в логах `kafka-init`. После создания топиков этот контейнер завершается с кодом `0`. PostgreSQL готов к подключению, когда его статус в `docker compose ps` станет `healthy`.

| Сервис | Из сети Compose | С хоста |
|---|---|---|
| Kafka | `kafka-1:19092,kafka-2:19092,kafka-3:19092` | `localhost:29092,localhost:39092,localhost:49092` |
| PostgreSQL | `postgresql://homework:homework@postgres:5432/kafka_homework` | `postgresql://homework:homework@localhost:15432/kafka_homework` |

Из сети Compose можно проверить топики и PostgreSQL следующими командами.

```sh
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:19092 --describe
docker compose exec -T postgres psql -U homework -d kafka_homework -c 'SELECT 1;'
```

В выводе ожидаются `events` и `events.dlt` с тремя партициями и коэффициентом репликации 3. Для `events` также должно отображаться `min.insync.replicas=2`.

Для проверки Kafka с хоста можно использовать `kcat`:

```sh
kcat -b localhost:29092,localhost:39092,localhost:49092 -L
```

`docker compose down` останавливает стенд. Именованные тома сохраняют данные между запусками.

## Запуск продюсеров и консьюмеров

Профиль `pipeline` запускает три продюсера (`producer`, `producer-2`, `producer-3`) и три консьюмера (`consumer`, `consumer-2`, `consumer-3`). Образ использует userver 3.2 для `linux/amd64`. На Apple Silicon в Compose задан `QEMU_GUEST_BASE` для совместимости `jemalloc` с QEMU.

```sh
docker compose --profile pipeline build producer
docker compose --profile pipeline up -d
docker compose --profile pipeline ps
```

Параметры передаются окружением Compose. Например, `EVENTS_PER_SECOND=20 PROCESSING_DELAY_MS=10 docker compose --profile pipeline up -d` задаёт скорость и задержку для всех экземпляров.

| Параметр | По умолчанию | Назначение |
|---|---|---|
| `EVENTS_PER_SECOND` | `10` | Целевая скорость каждого продюсера |
| `KEY_MODE` | `user_id` | Ключ `user_id` или отправка без ключа с `none` |
| `PROCESSING_DELAY_MS` | `20` | Задержка одной попытки обработки |

Каждый продюсер обслуживает 12 пользователей со своим префиксом. При перезапуске создаётся новый набор пользователей, их `seq` начинаются с 1. Около 2% событий помечаются `payload.poison=true`. Продюсер использует `acks=all` и `enable.idempotence=true`. При `SIGTERM` он даёт завершиться уже начатой отправке.

Консьюмеры входят в группу `events-processor`. При отсутствии сохранённого оффсета они начинают с `earliest`. Успешная запись в PostgreSQL или подтверждённая публикация в DLT позволяет сохранить следующий оффсет. При штатном отзыве партиций консьюмер коммитит уже сохранённые оффсеты перед их передачей. Ошибки PostgreSQL или отправки в DLT приводят к повторной попытке без продвижения оффсета.

Ядовитое событие после трёх неудачных попыток отправляется в `events.dlt` с заголовками `error-reason`, `original-topic`, `original-partition` и `original-offset`. В PostgreSQL `processed_events` хранит результаты, а `sink_counters` хранит число повторных записей, отклонённых по `event_id`.

```sh
docker compose exec -T postgres psql -U homework -d kafka_homework \
  -c "SELECT value AS duplicates FROM sink_counters WHERE name = 'duplicates';"
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:19092 --describe --group events-processor
```

Остановить весь стенд можно командой `docker compose --profile pipeline down`.

## Сквозная проверка на одном событии

На запущенном стенде отправьте одно контрольное событие. Продюсер напечатает его `event_id` после подтверждения записи в Kafka.

```sh
event_id=$(docker compose --profile pipeline run --rm -T --no-deps -e SMOKE_ONCE=1 producer)
echo "$event_id"
```

Проверьте строку в базе по этому UUID. Если консьюмер ещё обрабатывает очередь, повторите запрос.

```sh
docker compose exec -T postgres psql -U homework -d kafka_homework -v event_id="$event_id" <<'SQL'
SELECT event_id, user_id, type, seq, produced_at, payload,
       consumer_id, kafka_topic, kafka_partition, kafka_offset
FROM processed_events
WHERE event_id = :'event_id'::uuid;
SQL
```

Ожидается строка с напечатанным `event_id`, `seq=1` и топиком `events`.
