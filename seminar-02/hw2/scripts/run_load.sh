#!/usr/bin/env bash
# Full deterministic run: fresh volumes -> producers for RUN_DURATION -> consumers
# idle-exit -> verify. Arguments are explicit, not hidden defaults.
# With RUN_DURATION=0 (keep-running mode) the run is bounded by WINDOW seconds and
# stopped with SIGTERM, see scripts/finish_run.sh.
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -f .env ]] || { echo 'Copy .env.example to .env and run python scripts/configure.py'; exit 2; }
LABEL=${1:-manual}; FRESH=${2:-1}; WINDOW=${3:-120}
[[ "$LABEL" =~ ^[a-zA-Z0-9_-]{1,60}$ ]] || { echo 'Unsafe run label' >&2; exit 2; }
[[ "$FRESH" == 0 || "$FRESH" == 1 ]] || { echo 'FRESH must be 0 or 1' >&2; exit 2; }
mkdir -p results docs/evidence
if [[ "$FRESH" == 1 ]]; then docker compose down -v >/dev/null 2>&1; fi
docker compose up -d --build
OUT="results/run-$(date -u +%Y%m%dT%H%M%SZ)-$LABEL"
mkdir -p "$OUT"
date -u +%FT%TZ > "$OUT/start.txt"
bash scripts/finish_run.sh "$WINDOW"
docker wait lab2-producer1 lab2-producer2 lab2-producer3 > "$OUT/producers-exit-codes.txt"
docker wait lab2-consumer1 lab2-consumer2 lab2-consumer3 > "$OUT/consumers-exit-codes.txt"
date -u +%FT%TZ > "$OUT/end.txt"
set +e
docker compose run --rm -T verify | tee "$OUT/verify.txt"
RC=${PIPESTATUS[0]}
set -e
cp "$OUT/verify.txt" "docs/evidence/verify-$LABEL.txt"
echo "run=$LABEL fresh=$FRESH window=$WINDOW verify exit=$RC; evidence: $OUT" >&2
exit "$RC"
