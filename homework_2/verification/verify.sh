#!/bin/bash

set -eou pipefail

echo "-----------------------------------------------------------------------"
printf "Student: https://github.com/AlexanderUp, %s\n" "$(date "+%a %d %b %Y %H:%M:%S %Z")"
echo "--------------------- Verification script started ---------------------"
echo -n "1. Duplicated sequence count: "
docker compose exec postgres psql -U postgres --dbname=events_database -t -A -c "$(cat ./verification/seq_duplicates.sql)"

echo -n "2. Sequence gaps count: "
docker compose exec postgres psql -U postgres --dbname=events_database -t -A -c "$(cat ./verification/seq_gaps.sql)"

echo -n "3a. Poisonous msgs in DB count: "
docker compose exec postgres psql -U postgres --dbname=events_database -t -A -c "$(cat ./verification/poison_msgs_in_db.sql)"
echo -n "3b. DLT messages count in Kafka cluster: "
docker compose exec kafka-0 bash -c '/opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic events.dlt' | awk -F: 'BEGIN {sum=0} {sum+=$3} END { print sum }'

echo -n "4. Deduplicated events count: "
docker compose exec postgres psql -U postgres --dbname=events_database -t -A -c "$(cat ./verification/deduplicated_events_count.sql)"

echo -n "5. Order breaks count: "
docker compose exec postgres psql -U postgres --dbname=events_database -t -A -c "$(cat ./verification/order_breaks.sql)"

echo "--------------------- Verification script finished --------------------"
echo "-----------------------------------------------------------------------"
