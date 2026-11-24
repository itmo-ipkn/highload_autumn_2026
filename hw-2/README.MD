# HW2 — Kafka

Первый этап: три узла `apache/kafka:4.1.1` в режиме KRaft, каждый совмещает роли broker и controller. ZooKeeper не используется. Нужны Docker Compose и свободные порты 19092, 29092, 39092. Все команды ниже выполняются из `hw-2`.

## Запуск

```bash
docker compose up -d
docker compose ps -a
docker compose logs kafka-init
```

Можно запускать и обычным `docker compose up`, чтобы видеть логи. Брокеры стартуют параллельно; после простого API healthcheck всех трёх узлов одноразовый `kafka-init` создаёт топики и завершается с кодом 0 (`Exited (0)` в `ps -a`). Дождитесь его завершения перед отправкой сообщений.

| Топик | Партиции | Replication factor | min.insync.replicas |
|---|---:|---:|---:|
| `events` | 3 | 3 | 2 |
| `events.dlt` | 3 | 3 | 1 (по умолчанию) |

Автосоздание топиков отключено. Init использует `--if-not-exists`: существующие топики не пересоздаются и их параметры автоматически не исправляются.

Общие настройки узлов вынесены в YAML anchors в Compose. У каждого узла свой `node.id` и volume; `CLUSTER_ID` и список `controller.quorum.voters` общие. Официальный entrypoint выбранной версии форматирует хранилище для статического quorum; динамический bootstrap не используется.

Heap каждого брокера по умолчанию ограничен `-Xms256m -Xmx512m`. При необходимости переопределить:

```bash
KAFKA_HEAP_OPTS='-Xms256m -Xmx768m' docker compose up -d
```

## Адреса подключения

| Узел | Из Docker Compose (INTERNAL) | С хоста (EXTERNAL) |
|---|---|---|
| kafka-1 | `kafka-1:9092` | `localhost:19092` |
| kafka-2 | `kafka-2:9092` | `localhost:29092` |
| kafka-3 | `kafka-3:9092` | `localhost:39092` |

Controller listener использует порт 9093 внутри Compose. В `advertised.listeners` включены только INTERNAL и EXTERNAL: клиент получает адреса, доступные из его сети.

## Проверка кластера и топиков

Стандартные Kafka CLI уже есть в контейнерах:

```bash
docker compose exec kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka-1:9092
docker compose exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 describe --status
docker compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe
```

В первом выводе должны присутствовать брокеры с IDs 1, 2, 3, в quorum — три voters и выбранный `LeaderId`. В описании каждого учебного топика — три партиции с тремя репликами. `Leader` — брокер-лидер партиции, `Replicas` — назначенные реплики, `Isr` — синхронизированные реплики. В исправном кластере после синхронизации у каждой партиции три ISR. У `events` в `Configs` указан `min.insync.replicas=2`.

Если на хосте распакован дистрибутив Kafka 4.1.1 и установлена Java 17+, подключение с хоста проверяется теми же CLI (укажите свой путь):

```bash
KAFKA_BIN=/path/to/kafka/bin
"$KAFKA_BIN/kafka-broker-api-versions.sh" --bootstrap-server localhost:19092,localhost:29092,localhost:39092
```

В ответе будут внешние адреса трёх брокеров. Для работы через `docker compose exec` устанавливать Kafka или Java на хост не требуется.

## Отправка и чтение сообщения

```bash
printf 'hello-kafka\n' | docker compose exec -T kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:9092 --topic events --producer-property acks=all
docker compose exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic events --from-beginning --max-messages 1 --timeout-ms 10000
```

На новом стенде consumer напечатает `hello-kafka`. Если в топике уже есть сообщения, `--from-beginning --max-messages 1` прочитает одно из ранее записанных сообщений.

## Остановка

```bash
docker compose down
```

Named volumes сохраняются для следующего запуска и экспериментов. Полный сброс **с удалением данных Kafka**:

```bash
docker compose down -v
```
