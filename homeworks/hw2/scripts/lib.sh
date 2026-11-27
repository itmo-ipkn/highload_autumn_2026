#!/usr/bin/env bash
set -Eeuo pipefail

HW2_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$HW2_DIR"
WAIT_TIMEOUT_SECONDS=${WAIT_TIMEOUT_SECONDS:-300}
RUN_SECONDS=${RUN_SECONDS:-30}
producers=(producer-1 producer-2 producer-3)
consumers=(consumer-1 consumer-2 consumer-3)

begin_experiment() {
  command -v jq >/dev/null || { echo 'Install jq to run the experiments' >&2; return 1; }
  mkdir -p .cache/experiments
  ARTIFACT_DIR=$(mktemp -d "$HW2_DIR/.cache/experiments/$1-XXXXXXXX")
  COMPOSE_PROJECT_NAME="hw2-$(basename "$ARTIFACT_DIR" | tr '[:upper:]' '[:lower:]')"
  # Never connect experimental producers/consumers to a Kafka address from the host .env.
  export COMPOSE_PROJECT_NAME KAFKA_BROKERS=kafka-1:19092,kafka-2:19092,kafka-3:19092
  COMPOSE=(docker compose --env-file /dev/null -p "$COMPOSE_PROJECT_NAME" -f docker-compose.yaml -f scripts/experiments.compose.yaml)
  exec > >(tee "$ARTIFACT_DIR/run.txt") 2>&1
  echo "Experiment: $COMPOSE_PROJECT_NAME; artifacts: $ARTIFACT_DIR"
  local label="label=com.docker.compose.project=$COMPOSE_PROJECT_NAME" resources
  resources=$(docker ps -aq --filter "$label" && docker volume ls -q --filter "$label" && docker network ls -q --filter "$label")
  [[ -z $resources ]] || { echo 'Experiment project already exists; refusing to reset it' >&2; return 1; }
  trap cleanup_experiment EXIT
  "${COMPOSE[@]}" build secrets-init kafka-init postgres producer-1 consumer-1
}

cleanup_experiment() {
  "${COMPOSE[@]}" down -v --timeout 5
  local name
  for name in producer consumer kafka-init postgres secrets-init; do
    if docker image inspect "$COMPOSE_PROJECT_NAME-$name:local" >/dev/null 2>&1; then
      docker image rm "$COMPOSE_PROJECT_NAME-$name:local" >/dev/null
    fi
  done
}

reset_stack() {
  "${COMPOSE[@]}" down -v
  "${COMPOSE[@]}" up -d --wait --wait-timeout "$WAIT_TIMEOUT_SECONDS" --no-build "${producers[@]}" "${consumers[@]}"
}

stop_producers() {
  local pod id
  "${COMPOSE[@]}" stop "${producers[@]}"
  for pod in "${producers[@]}"; do
    id=$("${COMPOSE[@]}" ps -aq "$pod")
    [[ $(docker inspect -f '{{.State.ExitCode}} {{.RestartCount}}' "$id") == '0 0' ]] || { echo "Unexpected exit/restart: $pod" >&2; return 1; }
    "${COMPOSE[@]}" logs --no-color --no-log-prefix "$pod" | jq -Rse '
      [split("\n")[] | fromjson? | select(.msg == "producer stopped")] |
      if length == 1 and (.[0] | .enqueued > 0 and .acked == .enqueued and .seq_total == .acked and .failed == 0 and .unacked == 0)
      then .[0] | {pod,enqueued,acked,failed,unacked,seq_total}
      else error("producer drain not confirmed: \(.)") end'
  done
}

group_state() {
  "${COMPOSE[@]}" exec -T -e 'KAFKA_HEAP_OPTS=-Xms32m -Xmx128m' kafka-1 timeout 15 /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka-1:19092 --describe --group "${KAFKA_CONSUMER_GROUP:-events-processor}"
}

wait_for_zero_lag() {
  local deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS)) lag output
  local group=${KAFKA_CONSUMER_GROUP:-events-processor} topic=${EVENTS_TOPIC:-events}
  while (( SECONDS < deadline )); do
    if output=$(group_state); then
      lag=$(awk -v group="$group" -v topic="$topic" -f scripts/lag.awk <<< "$output")
      if [[ $lag == 0 ]]; then
        printf '%s\n' "$output" | tee "$ARTIFACT_DIR/lag-${KEY_MODE:-user_id}.txt"
        return 0
      fi
      echo "Current lag ($group/$topic): $lag"
    fi
    sleep 2
  done
  echo "Timed out waiting for $group/$topic to drain" >&2
  return 1
}

verify_passes() {
  "${COMPOSE[@]}" run --rm --no-deps -T verify
}

wait_for_seconds() {
  echo "Generating events for ${RUN_SECONDS}s..."
  sleep "$RUN_SECONDS"
}
