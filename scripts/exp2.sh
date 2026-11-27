#!/usr/bin/env bash
# Experiment 2: stop one broker, then a second one (ISR < min.insync.replicas), then bring them back.
set -u
cd "$(dirname "$0")/.."
log() { echo "### $(date +%T) $*"; }
topics() { scripts/kt.sh kafka-topics --describe --topic events 2>/dev/null | sed 's/\tElr.*//'; }
docker compose down -v >/dev/null 2>&1; docker compose up -d >/dev/null 2>&1; log "stack started"
sleep 60; log "baseline"; topics
log "docker compose stop kafka-2"; docker compose stop kafka-2 >/dev/null 2>&1
sleep 45; log "after stopping kafka-2"; topics
log "docker compose stop kafka-3"; docker compose stop kafka-3 >/dev/null 2>&1
sleep 30; log "after stopping kafka-3"; topics
sleep 60; log "producer buffers:"; docker compose logs --no-log-prefix --tail 1 producer-1 producer-2 producer-3
log "docker compose start kafka-3"; docker compose start kafka-3 >/dev/null 2>&1
sleep 40; log "after starting kafka-3"; topics
log "docker compose start kafka-2"; docker compose start kafka-2 >/dev/null 2>&1
sleep 60; log "after starting kafka-2"; topics
log "stopping producers"; docker compose stop producer-1 producer-2 producer-3 >/dev/null 2>&1
sleep 20; scripts/kt.sh group | tail -4
docker compose logs producer-1 producer-2 producer-3 | grep -c "DELIVERY FAILED" | sed 's/^/DELIVERY FAILED lines: /'
docker compose run --rm -q verify
