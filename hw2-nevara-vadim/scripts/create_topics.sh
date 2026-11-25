#!/bin/sh
set -eu

kafka_topics=/opt/kafka/bin/kafka-topics.sh

create_topic() {
    topic=$1
    shift
    attempt=1
    until timeout 15s "$kafka_topics" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
        --create --if-not-exists --topic "$topic" \
        --partitions 3 --replication-factor 3 "$@"
    do
        if [ "$attempt" -ge 8 ]; then
            echo "Could not create topic $topic" >&2
            return 1
        fi
        attempt=$((attempt + 1))
        sleep 2
    done
}

create_topic events --config min.insync.replicas=2
create_topic events.dlt

timeout 15s "$kafka_topics" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --describe --topic 'events(\.dlt)?'
