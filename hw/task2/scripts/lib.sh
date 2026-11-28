#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

RESULTS_DIR="${RESULTS_DIR:-${ROOT_DIR}/docs/results}"
mkdir -p "${RESULTS_DIR}"

COMPOSE=(docker compose)
KAFKA_BIN="/opt/kafka/bin"
GROUP="${CONSUMER_GROUP:-events-processor}"

say() {
  printf '%s\n' "$*"
}

section() {
  printf '\n=== %s ===\n' "$*"
}

log() {
  printf '%s\n' "$*"
}

compose_services() {
  "${COMPOSE[@]}" ps --services --status running 2>/dev/null || true
}

wait_for_brokers() {
  local timeout="${1:-180}" waited=0 healthy status
  while [ "${waited}" -lt "${timeout}" ]; do
    healthy=0
    for broker in kafka-broker-1 kafka-broker-2 kafka-broker-3; do
      status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}missing{{end}}' \
        "${broker}" 2>/dev/null || true)"
      [ "${status}" = "healthy" ] && healthy=$((healthy + 1))
    done
    [ "${healthy}" -eq 3 ] && return 0
    sleep 3
    waited=$((waited + 3))
  done
  return 1
}

wait_for_group_members() {
  local expected="${1:-3}" timeout="${2:-180}" waited=0 members
  while [ "${waited}" -lt "${timeout}" ]; do
    members="$(group_members)"
    [ "${members}" -ge "${expected}" ] && return 0
    sleep 3
    waited=$((waited + 3))
  done
  return 1
}

wait_for_drain() {
  local timeout="${1:-180}" waited=0 lag
  while [ "${waited}" -lt "${timeout}" ]; do
    lag="$(total_lag | tr -d ' ')"
    [ "${lag}" = "0" ] && return 0
    sleep 3
    waited=$((waited + 3))
  done
  return 1
}

group_raw() {
  local attempt raw
  for attempt in 1 2 3 4 5; do
    raw="$("${COMPOSE[@]}" exec -T kafka-broker-1 "${KAFKA_BIN}/kafka-consumer-groups.sh" \
      --bootstrap-server localhost:9092 --describe --group "${GROUP}" 2>/dev/null || true)"
    if printf '%s\n' "${raw}" | grep -q '^GROUP'; then
      printf '%s\n' "${raw}"
      return 0
    fi
    sleep 2
  done
  return 1
}

group_members() {
  group_raw 2>/dev/null \
    | awk '$3 ~ /^[0-9]+$/ && $9 ~ /^events-processor/ {print $9}' \
    | sort -u | wc -l | tr -d ' ' || true
}

total_lag() {
  group_raw 2>/dev/null \
    | awk '$3 ~ /^[0-9]+$/ && $6 ~ /^[0-9-]+$/ && $6 >= 0 {sum += $6} END {print sum + 0}' \
    || true
}

topic_raw() {
  "${COMPOSE[@]}" exec -T kafka-broker-3 timeout 45 "${KAFKA_BIN}/kafka-topics.sh" \
    --bootstrap-server localhost:9092 --describe --topic events 2>/dev/null || true
}

pause_producers() {
  "${COMPOSE[@]}" stop producer-1 producer-2 producer-3 >/dev/null 2>&1 || true
}

resume_producers() {
  "${COMPOSE[@]}" start producer-1 producer-2 producer-3 >/dev/null 2>&1 || true
}

run_verify() {
  local output="${1:-${RESULTS_DIR}/verify-manual-$(date +%Y%m%d-%H%M%S).txt}"
  "${COMPOSE[@]}" --profile tools build verify >/dev/null 2>&1 || true
  "${COMPOSE[@]}" --profile tools run --rm --no-deps verify >"${output}" 2>/dev/null || true
  cat "${output}"
}

start_stack() {
  local mode="${1:-user_id}"
  KEY_MODE="${mode}" "${COMPOSE[@]}" up -d --build >/dev/null 2>&1
  wait_for_brokers 180
  wait_for_group_members 3 240
}
