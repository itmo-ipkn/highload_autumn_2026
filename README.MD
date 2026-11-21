# Kafka lab

## Кластер

```sh
docker compose up -d
docker compose ps
docker compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic events
docker compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic events.dlt
```

Три узла Kafka работают в режиме KRaft с совмещёнными ролями broker и controller. Внутри сети Compose клиенты подключаются к `kafka-1:9092,kafka-2:9092,kafka-3:9092`; с хоста — к `localhost:19092,localhost:19093,localhost:19094`. Сервис `topics-init` создаёт топики при запуске; автоматическое создание отключено.
