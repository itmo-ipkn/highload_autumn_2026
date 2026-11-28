#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
MODE="${1:-user_id}"
log "Removing containers and volumes"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
start_stack "${MODE}"
log "Clean stack is ready"
