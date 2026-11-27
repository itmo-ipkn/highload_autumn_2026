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

## Сквозная проверка на одном событии

Из каталога `homework2` соберите общий образ для C++-продюсера и консьюмера и запустите консьюмер. Образ использует userver 3.2 для `linux/amd64`. На Apple Silicon нужна поддержка запуска `amd64`-контейнеров. В Compose задан `QEMU_GUEST_BASE` для совместимости `jemalloc` с адресным пространством QEMU.

```sh
docker compose --profile smoke build producer
docker compose --profile smoke up -d consumer
event_id=$(docker compose --profile smoke run --rm -T --no-deps producer)
echo "$event_id"
```

Продюсер генерирует одно событие `page_view` с новым UUID и печатает `event_id` после подтверждения записи в Kafka. Консьюмер создаёт таблицу `processed_events` при запуске, записывает туда событие и сохраняет оффсет.

После появления `Stored event` с этим UUID в `docker compose --profile smoke logs consumer` проверьте строку в базе.

```sh
docker compose exec -T postgres psql -U homework -d kafka_homework -v event_id="$event_id" <<'SQL'
SELECT event_id, user_id, type, seq, produced_at, payload,
       consumer_id, kafka_topic, kafka_partition, kafka_offset
FROM processed_events
WHERE event_id = :'event_id'::uuid;
SQL
```

Ожидается строка с напечатанным `event_id`, `seq=1` и топиком `events`. Новый запуск продюсера генерирует новое событие. Команда `docker compose --profile smoke down` останавливает также консьюмер.
