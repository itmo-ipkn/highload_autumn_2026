#!/bin/bash
set -euo pipefail

cli=/opt/kafka/bin/kafka-topics.sh
brokers=kafka-1:9092,kafka-2:9092,kafka-3:9092
for topic in events events.dlt; do
  "$cli" --bootstrap-server "$brokers" --create --if-not-exists \
    --topic "$topic" --partitions 3 --replication-factor 3 \
    --config min.insync.replicas=2 --config cleanup.policy=delete
  "$cli" --bootstrap-server "$brokers" --describe --topic "$topic"
done
