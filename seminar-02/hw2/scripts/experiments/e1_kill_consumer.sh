#!/usr/bin/env bash
# Experiment 1: kill -9 one consumer during a run.
# HYPOTHESIS: the group rebalances within ~session.timeout.ms (6 s); surviving
# consumers pick up the released partitions, resume from the LAST COMMITTED offset
# of that partition, so the killed pod's uncommitted in-flight batch is redelivered
# (at-least-once) and suppressed by event_id dedup. Nothing is lost; duplicates are
# bounded by one batch, and poison attempts continue from the persisted attempt rows.
set -euo pipefail
# Git Bash (MSYS) rewrites POSIX-looking arguments like /opt/kafka/bin/... into
# Windows paths before passing them to docker.exe; disable that for this script.
# Ignored on Linux/macOS.
export MSYS2_ARG_CONV_EXCL='*'
cd "$(dirname "$0")/../.."
[[ -f .env ]] || { echo 'Copy .env.example to .env and run python scripts/configure.py'; exit 2; }
TARGET=${1:-consumer2}
[[ "$TARGET" =~ ^consumer[123]$ ]] || { echo 'TARGET must be consumer1|consumer2|consumer3' >&2; exit 2; }
mkdir -p results docs/evidence
echo 'HYPOTHESIS: kill -9 consumer -> rebalance <= session.timeout.ms, resume from last committed offset, redelivery duplicates bounded by one batch, no loss.'
docker compose down -v >/dev/null 2>&1
docker compose up -d --build
OUT="results/e1-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUT"
sleep 12
date -u +%FT%TZ > "$OUT/kill-time.txt"
docker kill -s KILL "lab2-$TARGET" > "$OUT/killed.txt"
# The killed pod exits with 137 by design (kill -9); `docker wait` propagates that
# as its own exit status, so guard with `|| true` and keep the codes as evidence.
bash scripts/finish_run.sh "${WINDOW:-120}"
docker wait lab2-producer1 lab2-producer2 lab2-producer3 > "$OUT/producers-exit-codes.txt"
docker wait lab2-consumer1 lab2-consumer2 lab2-consumer3 > "$OUT/consumers-exit-codes.txt" || true
docker compose exec -T broker1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group events-processor > "$OUT/consumer-group.txt"
docker logs "lab2-$TARGET" > "$OUT/killed-pod.log" 2>&1 || true
for p in 1 2 3; do
  [[ "consumer$p" == "$TARGET" ]] && continue
  docker logs "lab2-consumer$p" > "$OUT/consumer$p.log" 2>&1
done
set +e
docker compose run --rm -T verify > "$OUT/verify.txt"
RC=$?
set -e
cp "$OUT/verify.txt" docs/evidence/e1-verify.txt
grep -E '"event": "(assign|revoke_commit)"' "$OUT"/consumer*.log > "$OUT/rebalance-events.txt" || true
echo "Experiment 1 done; evidence: $OUT; verify exit=$RC" >&2
exit "$RC"
