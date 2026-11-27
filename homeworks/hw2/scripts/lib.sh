#!/usr/bin/env bash
set -Eeuo pipefail

HW2_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$HW2_DIR"

COMPOSE=(docker compose)
WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-300}"
RUN_SECONDS="${RUN_SECONDS:-30}"

reset_stack() {
  make reset
}

stop_producers() {
  "${COMPOSE[@]}" stop producer-1 producer-2 producer-3
}

wait_for_zero_lag() {
  local deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))
  local output lag
  echo "Waiting for consumer group events-processor to drain (timeout ${WAIT_TIMEOUT_SECONDS}s)..."
  while (( SECONDS < deadline )); do
    output=$("${COMPOSE[@]}" exec -T kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server kafka-1:19092,kafka-2:19092,kafka-3:19092 \
      --describe --group events-processor 2>/dev/null || true)
    lag=$(printf '%s\n' "$output" | awk '
      $1 == "events" && $2 ~ /^[0-9]+$/ && $5 ~ /^[0-9]+$/ { total += $5; rows++ }
      END { if (rows > 0) print total; else print "unknown" }
    ')
    if [[ "$lag" == 0 ]]; then
      echo "Consumer lag is zero."
      return 0
    fi
    echo "Current lag: $lag"
    sleep 5
  done
  echo "Timed out waiting for the consumer group to drain." >&2
  "${COMPOSE[@]}" exec -T kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka-1:19092,kafka-2:19092,kafka-3:19092 \
    --describe --group events-processor || true
  return 1
}

verify_passes() {
  "${COMPOSE[@]}" run --rm verify
}

wait_for_seconds() {
  echo "Generating events for ${RUN_SECONDS}s..."
  sleep "$RUN_SECONDS"
}
