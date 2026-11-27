#!/usr/bin/env bash
# Эксперимент 1: kill -9 консьюмера под нагрузкой.
EXP=1
. "$(dirname "$0")/lib.sh"

fresh 60

groups kafka-1 >"$RESULTS/1-groups-before.txt"
docker compose run --rm -T verify </dev/null >"$RESULTS/1-before.txt" || true

log "kill -9 consumer-2"
mark kill
docker kill -s KILL hw2-consumer-2-1 >/dev/null
docker inspect hw2-consumer-2-1 --format 'consumer-2 после kill: status={{.State.Status}} exit={{.State.ExitCode}} restarts={{.RestartCount}}' \
  >"$RESULTS/1-container.txt"

log "wait 90s: session timeout, rebalance, catch-up"
sleep 90
groups kafka-1 >"$RESULTS/1-groups-after.txt"
docker inspect hw2-consumer-2-1 --format 'consumer-2 через 90 с: status={{.State.Status}} restarts={{.RestartCount}}' \
  >>"$RESULTS/1-container.txt"

stop_and_verify "$RESULTS/1.txt"

logs_grep 'partitions (assigned|revoked|lost)|partition resumed|"start"' "$RESULTS/1-rebalance.log" \
  consumer-1 consumer-2 consumer-3
sql 'SELECT consumer_id, partition, count(*) AS duplicates, min("offset"), max("offset")
     FROM duplicates GROUP BY 1, 2 ORDER BY 1, 2' >"$RESULTS/1-duplicates.txt"
log "done"
