#!/bin/bash

set -eou pipefail

topics=("events" "events.dlt")

cd "/opt/kafka/bin"
echo "Start topic creation..."

for topic in ${topics[*]}; do
    ./kafka-topics.sh --bootstrap-server kafka-0:9092,kafka-1:9092,kafka-2:9092 \
    --create \
    --topic ${topic} \
    --if-not-exists \
    --partitions 3 \
    --replication-factor 3 \
    --config min.insync.replicas=2
done

echo "Complete topic creation."
