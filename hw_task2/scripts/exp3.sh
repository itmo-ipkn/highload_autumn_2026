#!/usr/bin/env bash
# Experiment 3: same load with KEY_MODE=user_id and KEY_MODE=none, verify after each.
#   scripts/exp3.sh user_id   |   scripts/exp3.sh none
set -u
cd "$(dirname "$0")/.."
mode=${1:?user_id|none}
log() { echo "### $(date +%T) $*"; }
docker compose down -v >/dev/null 2>&1; KEY_MODE=$mode docker compose up -d >/dev/null 2>&1; log "stack started, KEY_MODE=$mode"
sleep 120; log "stopping producers"; docker compose stop producer-1 producer-2 producer-3 >/dev/null 2>&1
sleep 20; scripts/kt.sh group | tail -4
docker compose run --rm -q verify
