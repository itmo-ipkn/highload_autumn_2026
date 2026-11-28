#!/usr/bin/env bash
set -Eeuo pipefail
source "$(dirname -- "$0")/lib.sh"

begin_experiment exp2
reset_stack
wait_for_seconds

describe() {
  "${COMPOSE[@]}" exec -T -e 'KAFKA_HEAP_OPTS=-Xms32m -Xmx128m' kafka-1 timeout 10 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic "${EVENTS_TOPIC:-events}"
}
wait_isr() {
  local n=$1 path=$2 deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))
  while (( SECONDS < deadline )); do
    if describe > "$path" && awk -v n="$n" '/Partition:/ {for(i=1;i<NF;i++) if ($i=="Isr:" && split($(i+1),a,",")==n) ok++} END {exit ok!=3}' "$path"; then cat "$path"; return; fi
    sleep 1
  done
  echo "Did not observe ISR size $n on all partitions" >&2; return 1
}
sample() {
  local pod port
  date -u +%FT%TZ
  for pod in producer-{1,2,3} consumer-{1,2,3}; do
    port=2112; [[ $pod != consumer-* ]] || port=2113
    printf '%s\n' "$pod"
    "${COMPOSE[@]}" exec -T "$pod" wget -qO- "http://localhost:$port/metrics" | grep -E '^(producer_messages_(acked|enqueued)_total|producer_in_flight_messages|consumer_events_processed_total)[{ ]'
  done
}
pair() {
  sample > "$ARTIFACT_DIR/$1-start.txt"
  sleep "${BROKER_OUTAGE_SECONDS:-10}"
  sample > "$ARTIFACT_DIR/$1-end.txt"
}
wait_isr 3 "$ARTIFACT_DIR/isr-before.txt"
"${COMPOSE[@]}" stop -t 5 kafka-2
wait_isr 2 "$ARTIFACT_DIR/isr-one-down.txt"
pair one-down
"${COMPOSE[@]}" stop -t 5 kafka-3
# The surviving broker may return stale metadata or the request can time out.
if describe > "$ARTIFACT_DIR/isr-two-down.txt" 2>&1; then echo 'describe status=0'; else echo "describe status=$?"; fi | tee -a "$ARTIFACT_DIR/isr-two-down.txt"
pair two-down
"${COMPOSE[@]}" start kafka-2 kafka-3
wait_isr 3 "$ARTIFACT_DIR/isr-restored.txt"
wait_for_seconds
stop_producers 2>&1 | tee "$ARTIFACT_DIR/exp2-producers.txt"
wait_for_zero_lag
verify_passes 2>&1 | tee "$ARTIFACT_DIR/exp2-verify.txt"
