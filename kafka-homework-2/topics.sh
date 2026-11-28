#!/bin/bash
set -euo pipefail

bootstrap=broker-1:9092,broker-2:9092,broker-3:9092
topics=/opt/kafka/bin/kafka-topics.sh

"$topics" --bootstrap-server "$bootstrap" --create --if-not-exists \
    --topic events --partitions 3 --replication-factor 3 --config min.insync.replicas=2
"$topics" --bootstrap-server "$bootstrap" --create --if-not-exists \
    --topic events.dlt --partitions 3 --replication-factor 3 --config min.insync.replicas=2
"$topics" --bootstrap-server "$bootstrap" --describe --topic 'events(\.dlt)?'
