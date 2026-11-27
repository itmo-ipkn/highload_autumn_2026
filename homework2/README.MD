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
