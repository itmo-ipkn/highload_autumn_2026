#!/bin/bash
set -euo pipefail

BOOTSTRAP="kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092"
KAFKA_BIN="/opt/kafka/bin"

echo "Waiting for the Kafka metadata API on ${BOOTSTRAP}..."
for attempt in $(seq 1 60); do
  if "${KAFKA_BIN}/kafka-broker-api-versions.sh" --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; then
    echo "Kafka is reachable."
    break
  fi
  if [ "${attempt}" -eq 60 ]; then
    echo "Kafka did not become ready in time" >&2
    exit 1
  fi
  sleep 2
done

create_topic() {
  local topic="$1"
  echo "Creating topic ${topic} (partitions=3, replication-factor=3, min.insync.replicas=2)..."
  "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists --topic "${topic}" \
    --partitions 3 --replication-factor 3 \
    --config min.insync.replicas=2

  "${KAFKA_BIN}/kafka-configs.sh" --bootstrap-server "${BOOTSTRAP}" \
    --alter --entity-type topics --entity-name "${topic}" \
    --add-config min.insync.replicas=2
}

create_topic "events"
create_topic "events.dlt"

echo "Topic descriptions:"
"${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --describe --topic events
"${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --describe --topic events.dlt

for topic in events events.dlt; do
  partitions=$("${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" \
    --describe --topic "${topic}" | awk -F'PartitionCount: ' 'NR==1 {split($2,a,/[^0-9]/); print a[1]}')
  if [ "${partitions}" != "3" ]; then
    echo "Topic ${topic} has ${partitions:-unknown} partitions, expected 3" >&2
    exit 1
  fi
done

echo "Kafka topics are ready."
