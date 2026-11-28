#!/usr/bin/env bash

set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

VICTIM="${1:-consumer-events-processor-2}"
STAMP="$(date +%Y%m%d-%H%M%S)"
BEFORE="${RESULTS_DIR}/exp1-before-${STAMP}.txt"
EARLY="${RESULTS_DIR}/exp1-after-${STAMP}.txt-early"
AFTER="${RESULTS_DIR}/exp1-after-${STAMP}.txt"
RECOVERED="${RESULTS_DIR}/exp1-after-${STAMP}.txt-recovered"
VERIFY="${RESULTS_DIR}/exp1-verify-${STAMP}.txt"

section "kill -9 ${VICTIM}"

if ! compose_services | grep -qx "${VICTIM}"; then
  start_stack user_id
fi

echo "--- consumer group before ---"
group_raw | tee "${BEFORE}" || true

echo "--- kill -9 ${VICTIM} ---"
docker update --restart=no "${VICTIM}" >/dev/null 2>&1
docker kill --signal=SIGKILL "${VICTIM}" >/dev/null 2>&1

sleep 8
echo "--- consumer group +8s (session timeout not expired) ---"
group_raw | tee "${EARLY}" || true

sleep 35
echo "--- consumer group +43s (after rebalance) ---"
group_raw | tee "${AFTER}" || true

docker update --restart=unless-stopped "${VICTIM}" >/dev/null 2>&1
"${COMPOSE[@]}" start "${VICTIM}" >/dev/null 2>&1
wait_for_group_members 3 180 || true

sleep 5
echo "--- consumer group after restart ---"
group_raw | tee "${RECOVERED}" || true

pause_producers
wait_for_drain 120 || true
resume_producers

section "verify"
run_verify "${VERIFY}"
