#!/usr/bin/env bash
# Experiment 3: per-user ordering with key=user_id vs key_mode=none.
# HYPOTHESIS: with KEY_MODE=user_id every event of a user goes to one partition and
# is processed in seq order (inversions = 0). With KEY_MODE=none events of the same
# user are spread across partitions by the random partitioner and consumed concurrently, so
# sink write order no longer matches seq (inversions > 0, still no gaps).
# Fresh volumes are required: per-user counters, committed offsets and dedup rows
# carry state between runs, so results are only interpretable on a clean stand.
set -euo pipefail
cd "$(dirname "$0")/../.."
[[ -f .env ]] || { echo 'Copy .env.example to .env and run python scripts/configure.py'; exit 2; }
MODE=${1:?usage: e3_key_mode.sh user_id|none}
[[ "$MODE" == user_id || "$MODE" == none ]] || { echo 'MODE must be user_id or none' >&2; exit 2; }
mkdir -p results docs/evidence
echo "HYPOTHESIS: keyed run -> 0 inversions; KEY_MODE=none -> inversions > 0; both runs lose nothing."
docker compose down -v >/dev/null 2>&1
KEY_MODE="$MODE" docker compose up -d --build
OUT="results/e3-$MODE-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUT"
bash scripts/finish_run.sh "${WINDOW:-120}"
docker wait lab2-producer1 lab2-producer2 lab2-producer3 > "$OUT/producers-exit-codes.txt"
docker wait lab2-consumer1 lab2-consumer2 lab2-consumer3 > "$OUT/consumers-exit-codes.txt"
set +e
docker compose run --rm -T verify > "$OUT/verify.txt"
RC=$?
set -e
cp "$OUT/verify.txt" "docs/evidence/e3-$MODE-verify.txt"
echo "Experiment 3 ($MODE) done; evidence: $OUT; verify exit=$RC" >&2
exit "$RC"
