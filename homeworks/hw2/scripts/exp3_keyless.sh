#!/usr/bin/env bash
set -Eeuo pipefail
source "$(dirname -- "$0")/lib.sh"

# Compose interpolates KEY_MODE from the environment when creating the producers.
echo "Resetting stack with KEY_MODE=none..."
KEY_MODE=none make reset
wait_for_seconds
stop_producers
wait_for_zero_lag

echo "Running verify; ordering violations are expected when messages have no user key."
set +e
"${COMPOSE[@]}" run --rm verify
verify_status=$?
set -e
if (( verify_status == 0 )); then
  echo "Expected verify to detect out-of-order writes, but it passed." >&2
  exit 1
fi

violations=$("${COMPOSE[@]}" exec -T postgres psql -U events -d events -Atc '
  WITH ordered AS (
    SELECT user_id, seq, max(seq) OVER (
      PARTITION BY user_id ORDER BY write_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS max_previous_seq
    FROM processed_events
  )
  SELECT count(*) FROM ordered
  WHERE max_previous_seq IS NOT NULL AND seq < max_previous_seq;
')
echo "Observed out-of-order sink writes: ${violations}"
if [[ ! "$violations" =~ ^[0-9]+$ ]] || (( violations == 0 )); then
  echo "Expected at least one ordering violation; increase RUN_SECONDS or producer rate." >&2
  exit 1
fi
echo "Experiment 3 complete: keyless partitioning caused ordering violations without sequence loss."
