#!/usr/bin/env bash

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

STAMP="$(date +%Y%m%d-%H%M%S)"
OUT="${RESULTS_DIR}/dedup-verify-${STAMP}.txt"

cleanup() {
  "${COMPOSE[@]}" start consumer-events-processor-1 consumer-events-processor-2 \
    consumer-events-processor-3 producer-1 producer-2 producer-3 >/dev/null 2>&1 || true
}
trap cleanup EXIT

section "deduplication check"

if ! compose_services | grep -qx "consumer-events-processor-1"; then
  start_stack user_id
fi

pause_producers
wait_for_drain 180 || true

"${COMPOSE[@]}" stop consumer-events-processor-1 consumer-events-processor-2 \
  consumer-events-processor-3 >/dev/null 2>&1

current=""
for _ in 1 2 3 4 5 6 7 8 9 10; do
  current="$(group_raw 2>/dev/null \
    | awk '$3 == 0 && $4 ~ /^[0-9]+$/ {print $4; exit}' || true)"
  [ -n "${current}" ] && break
  sleep 2
done
if [ -z "${current}" ] || [ "${current}" -lt 5 ]; then
  echo "ERROR: cannot read a committed offset >= 5 for partition 0"
  exit 1
fi

target=$((current - 5))
echo "partition 0: committed offset ${current} -> rewind to ${target}"

"${COMPOSE[@]}" exec -T kafka-broker-1 "${KAFKA_BIN}/kafka-consumer-groups.sh" \
  --bootstrap-server localhost:9092 --group "${GROUP}" \
  --topic "events:0" --reset-offsets --to-offset "${target}" --execute \
  >"${OUT}.reset" 2>&1 || true
cat "${OUT}.reset"

"${COMPOSE[@]}" start consumer-events-processor-1 consumer-events-processor-2 \
  consumer-events-processor-3 >/dev/null 2>&1
wait_for_group_members 3 180 || true
sleep 10
wait_for_drain 120 || true

section "verify"
run_verify "${OUT}"
printf '\nSaved: %s\n' "${OUT}"
