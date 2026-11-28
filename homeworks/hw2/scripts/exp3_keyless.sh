#!/usr/bin/env bash
set -Eeuo pipefail
source "$(dirname -- "$0")/lib.sh"

# Equal per-pod budgets, not just equal sleeps (startup and drain can differ).
export MAX_EVENTS=${MAX_EVENTS:-500}
[[ $MAX_EVENTS =~ ^[1-9][0-9]*$ ]] || { echo 'MAX_EVENTS must be positive' >&2; exit 1; }
begin_experiment exp3
for mode in user_id none; do
  export KEY_MODE=$mode
  reset_stack
  deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))
  for pod in producer-{1,2,3}; do
    until "${COMPOSE[@]}" logs --no-log-prefix "$pod" | grep -F 'MAX_EVENTS reached' > /dev/null; do
      (( SECONDS < deadline )) || { echo "$pod did not reach MAX_EVENTS" >&2; exit 1; }
      sleep 1
    done
  done
  stop_producers 2>&1 | tee "$ARTIFACT_DIR/$mode-producers.txt"
  wait_for_zero_lag
  rc=0
  "${COMPOSE[@]}" run --rm --no-deps -T verify > "$ARTIFACT_DIR/$mode-verify.txt" 2>&1 || rc=$?
  cat "$ARTIFACT_DIR/$mode-verify.txt"
  printf 'KEY_MODE=%s verify exit=%d\n' "$mode" "$rc" | tee "$ARTIFACT_DIR/$mode-status.txt"
  case "$mode:$rc" in
    user_id:0) grep -q '^PASS:' "$ARTIFACT_DIR/$mode-verify.txt" ;;
    none:2) grep -Eq '^FAIL: .*order violations=[1-9][0-9]*, lag=0$' "$ARTIFACT_DIR/$mode-verify.txt" ;;
    *) echo "Unexpected verify result for $mode" >&2; exit 1 ;;
  esac
done
