#!/usr/bin/env bash
# Capture the SAME k6 command used manually. Arguments are not hidden defaults.
set -euo pipefail
cd "$(dirname "$0")/.."
LABEL=${1:-manual}; RATE=${2:-80}; DURATION=${3:-120s}; EXPECT_NORMAL=${4:-0}; VUS=${VUS:-160}
[[ "$LABEL" =~ ^[a-zA-Z0-9_-]{1,60}$ ]] || { echo 'Unsafe run label' >&2; exit 2; }
[[ "$RATE" =~ ^[1-9][0-9]{0,2}$ ]] && (( RATE <= 500 )) || { echo 'RATE must be 1..500' >&2; exit 2; }
[[ "$DURATION" =~ ^[1-9][0-9]{0,2}s$ ]] && (( ${DURATION%s} >= 5 && ${DURATION%s} <= 600 )) || { echo 'DURATION must be 5s..600s' >&2; exit 2; }
[[ "$VUS" =~ ^[1-9][0-9]{0,2}$ ]] && (( VUS <= 500 )) || { echo 'VUS must be 1..500' >&2; exit 2; }
[[ "$EXPECT_NORMAL" == 0 || "$EXPECT_NORMAL" == 1 ]] || { echo 'EXPECT_NORMAL must be 0 or 1' >&2; exit 2; }
mkdir -p results
# mkdir is atomic: two wrappers cannot silently run on the same stand at once.
mkdir results/.load.lock 2>/dev/null || { echo 'A run is already active; inspect docker compose --profile load ps before clearing a stale lock.' >&2; exit 2; }
trap 'rmdir results/.load.lock 2>/dev/null || true' EXIT
OUT=$(mktemp -d "results/$(date -u +%Y%m%dT%H%M%SZ)-$LABEL-XXXXXX")
printf '%s\n' "$OUT" > results/.last-run.txt
date -u +%FT%TZ > "$OUT/start.txt"
printf 'label=%s\nrate=%s\nduration=%s\nvus=%s\nexpect_normal=%s\n' "$LABEL" "$RATE" "$DURATION" "$VUS" "$EXPECT_NORMAL" > "$OUT/parameters.txt"
echo "Running $LABEL: $RATE iterations/s, $DURATION, $VUS VUs. Result: $OUT" >&2
set +e
docker compose --profile load run --rm -T --interactive=false k6 run --quiet \
  -e RATE="$RATE" -e DURATION="$DURATION" -e VUS="$VUS" \
  -e RUN_LABEL="$LABEL" -e EXPECT_NORMAL="$EXPECT_NORMAL" \
  /scripts/open.js > "$OUT/summary.json" 2> "$OUT/stderr.log"
RC=$?
set -e
date -u +%FT%TZ > "$OUT/end.txt"
printf '%s\n' "$RC" > "$OUT/exit-code.txt"
if ! python3 scripts/summarize.py "$OUT/summary.json"; then
  cat "$OUT/stderr.log" >&2
  # A bad/missing summary must not replace k6's failure code with success or another code.
  if (( RC != 0 )); then exit "$RC"; fi
  exit 1
fi
if (( RC != 0 )); then echo "k6 exit=$RC. Read thresholds/stderr. Failures are not suppressed." >&2; fi
exit "$RC"
