#!/usr/bin/env bash
set -Eeuo pipefail
source "$(dirname -- "$0")/lib.sh"

reset_stack
wait_for_seconds

echo "Killing consumer-1 with SIGKILL..."
"${COMPOSE[@]}" kill -s SIGKILL consumer-1
sleep 15
echo "Starting consumer-1 again..."
"${COMPOSE[@]}" up -d consumer-1
wait_for_seconds
stop_producers
wait_for_zero_lag
verify_passes

echo "Experiment 1 complete: verify should report no missing sequences or ordering violations."
