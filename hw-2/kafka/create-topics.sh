#!/usr/bin/env bash
# Создаёт топики и печатает их описание. Запускается сервисом kafka-init один раз.
set -euo pipefail

bs=kafka-1:19092,kafka-2:19092,kafka-3:19092
topics=/opt/kafka/bin/kafka-topics.sh

for _ in $(seq 60); do
  "$topics" --bootstrap-server "$bs" --list >/dev/null 2>&1 && break
  sleep 2
done

"$topics" --bootstrap-server "$bs" --create --if-not-exists --topic events \
  --partitions 3 --replication-factor 3 --config min.insync.replicas=2
"$topics" --bootstrap-server "$bs" --create --if-not-exists --topic events.dlt \
  --partitions 3 --replication-factor 3

"$topics" --bootstrap-server "$bs" --describe --topic 'events.*'
