#!/usr/bin/env bash

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

RUN_SECONDS="${1:-60}"
STAMP="$(date +%Y%m%d-%H%M%S)"
NONE_OUT="${RESULTS_DIR}/exp3-none-${STAMP}.txt"
USER_OUT="${RESULTS_DIR}/exp3-user-id-${STAMP}.txt"

run_mode() {
  local mode="$1" output="$2"

  section "KEY_MODE=${mode}"

  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  KEY_MODE="${mode}" "${COMPOSE[@]}" up -d --build >/dev/null 2>&1
  wait_for_brokers 180
  wait_for_group_members 3 240

  echo "warm-up 20s + load ${RUN_SECONDS}s..."
  sleep 20
  sleep "${RUN_SECONDS}"

  pause_producers
  wait_for_drain 180 || true

  run_verify "${output}"
}

section "KEY_MODE=none vs KEY_MODE=user_id (${RUN_SECONDS}s load per mode)"
run_mode none "${NONE_OUT}"
run_mode user_id "${USER_OUT}"

echo "--- restore KEY_MODE=user_id stack ---"
"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
KEY_MODE=user_id "${COMPOSE[@]}" up -d --build >/dev/null 2>&1
wait_for_brokers 180
wait_for_group_members 3 240
echo "stack restored"
