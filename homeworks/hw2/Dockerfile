# syntax=docker/dockerfile:1
# Образы инфраструктуры стенда. Конфиги вшиваются в образ, а не монтируются bind-mount.
# Так стенд одинаково поднимается и с хоста, и из devcontainer (docker-outside-of-docker),
# где пути проекта Docker-демону не видны.
# Продюсер и консьюмер собираются из своих Dockerfile в producer/ и consumer/.

ARG KAFKA_IMAGE=apache/kafka:4.3.2
ARG POSTGRES_IMAGE=postgres:16-alpine

# Одноразовое создание топиков.
FROM ${KAFKA_IMAGE} AS kafka-init
COPY --chmod=0555 deploy/kafka/create-topics.sh /create-topics.sh
ENTRYPOINT ["/bin/bash", "/create-topics.sh"]

# Postgres со схемой sink. Схема применяется при первом старте на пустом volume.
FROM ${POSTGRES_IMAGE} AS sink-postgres
COPY --chmod=0444 deploy/postgres/init.sql /docker-entrypoint-initdb.d/001-init.sql
