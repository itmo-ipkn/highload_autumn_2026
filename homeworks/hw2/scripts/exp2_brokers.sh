#!/usr/bin/env bash
set -Eeuo pipefail
source "$(dirname -- "$0")/lib.sh"

BROKER_OUTAGE_SECONDS="${BROKER_OUTAGE_SECONDS:-20}"
reset_stack
wait_for_seconds

echo "Stopping kafka-2 and kafka-3 (the KRaft controller quorum will be lost)..."
"${COMPOSE[@]}" stop kafka-2 kafka-3
sleep "$BROKER_OUTAGE_SECONDS"
echo "Restarting kafka-2 and kafka-3..."
"${COMPOSE[@]}" start kafka-2 kafka-3

echo "Waiting for both brokers to become healthy..."
"${COMPOSE[@]}" up -d --wait kafka-2 kafka-3
wait_for_seconds
stop_producers
wait_for_zero_lag
verify_passes

echo "Experiment 2 complete: verify should report no missing sequences after brokers recover."
