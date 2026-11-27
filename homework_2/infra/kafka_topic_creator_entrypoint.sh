#!/bin/bash

set -eou pipefail

topics=("events" "events.dlt")

cd "/opt/kafka/bin"
echo "Start topic creation..."

check_health() {
    /opt/kafka/bin/kafka-cluster.sh api-versions --bootstrap-server kafka-0:9092 1>/dev/null
}

while ! check_health; do
    echo "Kafka not ready, awaiting...";
    sleep 1;
done

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
