# syntax=docker/dockerfile:1
# Kafka topic init, credential bootstrap и Postgres schema включены в infra-образы.
# Продюсер и консьюмер собираются из своих Dockerfile в producer/ и consumer/.

ARG KAFKA_IMAGE=apache/kafka:4.3.2
ARG POSTGRES_IMAGE=postgres:16-alpine
ARG ALPINE_IMAGE=alpine:3.24.3

# Одноразовое создание топиков.
FROM ${KAFKA_IMAGE} AS kafka-init
COPY --chmod=0555 deploy/kafka/create-topics.sh /create-topics.sh
ENTRYPOINT ["/bin/bash", "/create-topics.sh"]

# Однократная генерация секретов в отдельном Docker volume.
FROM ${ALPINE_IMAGE} AS secrets-init
COPY --chmod=0555 deploy/secrets/init.sh /init-secrets.sh
ENTRYPOINT ["/bin/sh", "/init-secrets.sh"]

# Postgres со схемой sink. Схема применяется при первом старте на пустом volume.
FROM ${POSTGRES_IMAGE} AS sink-postgres
COPY --chmod=0444 deploy/postgres/init.sql /docker-entrypoint-initdb.d/001-init.sql
COPY --chmod=0555 deploy/postgres/with-password.sh /with-postgres-password.sh
ENTRYPOINT ["/with-postgres-password.sh"]
