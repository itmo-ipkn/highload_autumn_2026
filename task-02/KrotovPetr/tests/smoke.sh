#!/bin/bash
set -euo pipefail
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
export RUN_ID="smoke_$(date +%s)_$$"
export GITHUB_LOGIN=${GITHUB_LOGIN:-local-smoke}
export USERS_PER_PRODUCER=3 EVENTS_PER_USER=20 RATE_PER_SECOND=60
export PROCESSING_DELAY_MS=5 BEFORE_COMMIT_DELAY_MS=0 KEY_MODE=user_id POISON_RATE=0.02
mkdir -p "$root/artifacts/$RUN_ID"
printf 'RUN_ID=%s\nGITHUB_LOGIN=%s\n' "$RUN_ID" "$GITHUB_LOGIN" > "$root/artifacts/last-smoke.env"
"$root/local-compose" up -d --build
python3 "$root/tests/check_host_listener.py" | tee "$root/artifacts/$RUN_ID/host-listeners.txt"
"$root/local-compose" run --rm --no-deps verify --wait 180 | tee "$root/artifacts/$RUN_ID/verify.txt"
"$root/local-compose" run --rm --no-deps tests 2>&1 | tee "$root/artifacts/$RUN_ID/tests.txt"
"$root/local-compose" logs --no-color producer-1 producer-2 producer-3 consumer-1 consumer-2 consumer-3 \
  > "$root/artifacts/$RUN_ID/app.log"
printf 'Local smoke artifacts: %s\n' "$root/artifacts/$RUN_ID"
