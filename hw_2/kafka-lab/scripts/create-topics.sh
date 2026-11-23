#!/usr/bin/env bash
set -euo pipefail
K=/opt/kafka/bin/kafka-topics.sh
BOOTSTRAP=kafka-1:9092,kafka-2:9092,kafka-3:9092
for attempt in $(seq 1 90); do
  if "$K" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1 && \
     "$K" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
       --topic events --partitions 3 --replication-factor 3 \
       --config min.insync.replicas=2 >/dev/null 2>&1; then
    "$K" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
      --topic events.dlt --partitions 3 --replication-factor 3 \
      --config min.insync.replicas=2
    "$K" --bootstrap-server "$BOOTSTRAP" --describe --topic events
    "$K" --bootstrap-server "$BOOTSTRAP" --describe --topic events.dlt
    exit 0
  fi
  echo "Waiting for Kafka quorum attempt=$attempt"; sleep 3
done
echo 'Unable to create topics' >&2; exit 1
