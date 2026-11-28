#!/bin/bash

export PATH="/opt/kafka/bin:$PATH"

set -e

echo "Waiting for Kafka..."
until kafka-topics.sh --bootstrap-server kafka-1:9092 --list >/dev/null 2>&1; do
  sleep 3
done

echo "Creating topics..."

kafka-topics.sh --bootstrap-server kafka-1:9092 \
  --create --if-not-exists \
  --topic events \
  --partitions 3 \
  --replication-factor 3

kafka-configs.sh --bootstrap-server kafka-1:9092 \
  --entity-type topics \
  --entity-name events \
  --alter \
  --add-config min.insync.replicas=2

kafka-topics.sh --bootstrap-server kafka-1:9092 \
  --create --if-not-exists \
  --topic events.dlt \
  --partitions 3 \
  --replication-factor 3

echo "Topics created successfully"
