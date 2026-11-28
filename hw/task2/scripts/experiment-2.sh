#!/usr/bin/env bash

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

STAMP="$(date +%Y%m%d-%H%M%S)"
TOPICS_BEFORE="${RESULTS_DIR}/exp2-topics-before-${STAMP}.txt"
TOPICS_1DOWN="${RESULTS_DIR}/exp2-topics-one-broker-${STAMP}.txt"
TOPICS_2DOWN="${RESULTS_DIR}/exp2-topics-two-brokers-${STAMP}.txt"
TOPICS_RECOVERED="${RESULTS_DIR}/exp2-topics-recovered-${STAMP}.txt"
VERIFY="${RESULTS_DIR}/exp2-verify-${STAMP}.txt"

section "broker failures"

if ! compose_services | grep -qx "kafka-broker-1"; then
  start_stack user_id
fi

echo "--- events topic before ---"
topic_raw | tee "${TOPICS_BEFORE}" || true

echo "--- stop kafka-broker-1 ---"
"${COMPOSE[@]}" stop kafka-broker-1 >/dev/null 2>&1
sleep 15
echo "--- events topic after broker-1 down ---"
topic_raw | tee "${TOPICS_1DOWN}" || true

echo "--- stop kafka-broker-2 ---"
"${COMPOSE[@]}" stop kafka-broker-2 >/dev/null 2>&1
sleep 20
echo "--- events topic after broker-1 + broker-2 down ---"
topic_raw | tee "${TOPICS_2DOWN}" || true

echo "--- restore both brokers ---"
"${COMPOSE[@]}" start kafka-broker-1 kafka-broker-2 >/dev/null 2>&1
wait_for_brokers 240
sleep 30
wait_for_group_members 3 180 || true
echo "--- events topic after recovery ---"
topic_raw | tee "${TOPICS_RECOVERED}" || true

pause_producers
wait_for_drain 180 || true
resume_producers

section "verify"
run_verify "${VERIFY}"
