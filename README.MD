# Kafka lab

## Кластер

```sh
docker compose up -d
docker compose ps
docker compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic events
docker compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic events.dlt
```

Три узла Kafka работают в режиме KRaft с совмещёнными ролями broker и controller. Внутри сети Compose клиенты подключаются к `kafka-1:9092,kafka-2:9092,kafka-3:9092`; с хоста — к `localhost:19092,localhost:19093,localhost:19094`. Сервис `topics-init` создаёт топики при запуске; автоматическое создание отключено.

## Продюсеры

Три продюсера отправляют по 10 событий в секунду каждый. Один pod создаёт события одного `user_id`; при новом запуске pod получает новый UUID пользователя и начинает `seq` с 1. Каждое 50-е событие помечено `poison=true` (2%). Для сравнения распределения по партициям можно запустить стек с `KEY_MODE=none docker compose up -d --build`; по умолчанию ключ сообщения — `user_id`.

Клиент `franz-go` использует идемпотентную отправку по умолчанию. `acks=all` задан явно. Ошибки доставки выводятся в журнал pod; при SIGTERM продюсер перестаёт создавать события и ждёт отправки буфера до 40 секунд.

## Консьюмеры и sink

Три консьюмера составляют группу `events-processor` и записывают обработанные события в PostgreSQL. При первом запуске группа читает с начала топика (`earliest`); после этого использует сохранённые offsets. Автокоммит отключён: offset подтверждается после записи в sink или успешной отправки poison-события в `events.dlt`. Уникальный ключ `event_id` подавляет повторную запись; число таких попыток хранится в `duplicate_attempts`.

Каждое событие обрабатывается с задержкой 500 мс. Poison-событие трижды вызывает ошибку обработки, затем отправляется в `events.dlt` с причиной и исходными топиком, партицией и offset в заголовках.
