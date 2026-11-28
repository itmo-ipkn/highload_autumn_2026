#!/usr/bin/env bash
# Experiment 2: stop two brokers during a run (RF3 + min.insync.replicas=2).
# HYPOTHESIS: with two of three brokers down, partitions whose leader is on a down
# broker have < 2 in-sync replicas, so acks=all produces fail there (logged, retried);
# partitions led by the surviving broker have exactly 2 ISR members and keep working.
# Nothing is silently lost: failed sends stay in librdkafka's queue until the brokers
# return. After restart, ISR re-joins to 3 and committed offsets resume consumption.
set -euo pipefail
# Git Bash (MSYS) rewrites POSIX-looking arguments like /opt/kafka/bin/... into
# Windows paths before passing them to docker.exe; disable that for this script.
# Ignored on Linux/macOS.
export MSYS2_ARG_CONV_EXCL='*'
cd "$(dirname "$0")/../.."
[[ -f .env ]] || { echo 'Copy .env.example to .env and run python scripts/configure.py'; exit 2; }
STOP1=${1:-broker1}; STOP2=${2:-broker2}
[[ "$STOP1" =~ ^broker[123]$ && "$STOP2" =~ ^broker[123]$ && "$STOP1" != "$STOP2" ]] || { echo 'usage: e2_brokers.sh broker1 broker2' >&2; exit 2; }
mkdir -p results docs/evidence
echo 'HYPOTHESIS: RF3+MINSYNC2 tolerates one broker down; with two down, leader-down partitions stop accepting acks=all writes (errors logged, retried); after restart ISR re-joins and nothing is lost.'
docker compose down -v >/dev/null 2>&1
docker compose up -d --build
OUT="results/e2-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUT"
sleep 10
# Sequential shutdown: one broker at a time, so the ISR effect of each step is visible
# (the first stop leaves every partition with 2 ISR members, the second one drops the
# partitions whose leader was on the second broker to 1).
GAP=${GAP:-15}
date -u +%FT%TZ > "$OUT/stop-time.txt"
docker compose stop "$STOP1"
echo "stopped $STOP1, waiting $GAP s before stopping $STOP2" >&2
sleep "$GAP"
date -u +%FT%TZ >> "$OUT/stop-time.txt"
docker compose stop "$STOP2"
# A describe during a two-broker outage can time out on metadata for the stopped
# brokers; that timeout is itself evidence, so keep it and continue the run.
docker compose exec -T broker3 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic events > "$OUT/during-outage.txt" 2>&1 || echo "exit=$? (metadata request timed out while 2 of 3 brokers were down)" >> "$OUT/during-outage.txt"
sleep 10
date -u +%FT%TZ > "$OUT/restart-time.txt"
docker compose start "$STOP1" "$STOP2"
# wait for re-election + ISR re-join, then let producers finish naturally
# (WINDOW + SIGTERM when RUN_DURATION=0)
sleep 15
docker compose exec -T broker3 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic events > "$OUT/after-restart.txt"
bash scripts/finish_run.sh "${WINDOW:-120}"
docker wait lab2-producer1 lab2-producer2 lab2-producer3 > "$OUT/producers-exit-codes.txt"
docker wait lab2-consumer1 lab2-consumer2 lab2-consumer3 > "$OUT/consumers-exit-codes.txt"
for p in 1 2 3; do docker logs "lab2-producer$p" > "$OUT/producer$p.log" 2>&1; done
for p in 1 2 3; do docker logs "lab2-consumer$p" > "$OUT/consumer$p.log" 2>&1; done
grep -h '"event": "produce_error"' "$OUT"/producer*.log > "$OUT/produce-errors.txt" || true
set +e
docker compose run --rm -T verify > "$OUT/verify.txt"
RC=$?
set -e
cp "$OUT/verify.txt" docs/evidence/e2-verify.txt
echo "Experiment 2 done; evidence: $OUT; verify exit=$RC" >&2
exit "$RC"
