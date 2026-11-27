#!/usr/bin/env bash
# Experiment 1: kill -9 a consumer pod under load, then bring it back.
set -u
cd "$(dirname "$0")/.."
log() { echo "### $(date +%T) $*"; }
docker compose down -v >/dev/null 2>&1; docker compose up -d >/dev/null 2>&1; log "stack started"
sleep 75; log "before kill"; scripts/kt.sh group | tail -4
log "docker kill -s KILL consumer-2"; docker kill -s KILL "$(docker compose ps -q consumer-2)" >/dev/null
sleep 5;  log "+5s (consumer-2 still in the group until session.timeout.ms)"; scripts/kt.sh group | tail -4
sleep 55; log "+60s"; scripts/kt.sh group | tail -4
log "docker compose start consumer-2"; docker compose start consumer-2 >/dev/null 2>&1
sleep 40; log "after restart"; scripts/kt.sh group | tail -4
log "stopping producers (SIGTERM, flush)"; docker compose stop producer-1 producer-2 producer-3 >/dev/null 2>&1
sleep 15; scripts/kt.sh group | tail -4
docker compose logs --no-log-prefix consumer-1 consumer-2 consumer-3 | grep -E "REBALANCE|first message after|duplicate dropped" | sort -s -k1,1
docker compose run --rm -q verify
