#!/bin/bash
# Technical regression check, separate from the author's experiment report.
set -euo pipefail
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
export RUN_ID="recovery_$(date +%s)_$$"
export GITHUB_LOGIN=${GITHUB_LOGIN:-local-recovery}
export USERS_PER_PRODUCER=10 EVENTS_PER_USER=10 RATE_PER_SECOND=100
export POISON_RATE=0 KEY_MODE=user_id PROCESSING_DELAY_MS=5
mkdir -p "$root/artifacts/$RUN_ID"
restore_consumer() {
  BEFORE_COMMIT_DELAY_MS=0 "$root/local-compose" up -d --no-deps consumer-1
}
trap restore_consumer EXIT
BEFORE_COMMIT_DELAY_MS=5000 "$root/local-compose" up -d --no-deps consumer-1
BEFORE_COMMIT_DELAY_MS=0 "$root/local-compose" up -d --no-deps producer-1 producer-2 producer-3
paused=false
for ((attempt=0; attempt<60; attempt++)); do
  "$root/local-compose" logs --no-color consumer-1 > "$root/artifacts/$RUN_ID/before-kill.log"
  if python3 - "$root/artifacts/$RUN_ID/before-kill.log" <<'PY'
from pathlib import Path
import sys
raise SystemExit(0 if '"action": "before_commit_pause"' in Path(sys.argv[1]).read_text() else 1)
PY
  then
    paused=true
    break
  fi
  sleep 1
done
if [ "$paused" != true ]; then
  printf 'Consumer did not enter the expected pause\n' >&2
  exit 1
fi
"$root/local-compose" kill -s SIGKILL consumer-1
"$root/local-compose" run --rm --no-deps verify --wait 180 | tee "$root/artifacts/$RUN_ID/verify.txt"
python3 - "$root/artifacts/$RUN_ID/verify.txt" <<'PY'
import json
from pathlib import Path
import sys
text = Path(sys.argv[1]).read_text()
result = json.loads(text[text.index("\n{\n") + 1:])
if not result["ok"] or result["duplicates_discarded"] < 1:
    raise SystemExit("Recovery must preserve all events and demonstrate a suppressed duplicate")
print(f"Recovery passed: {result['expected']} events, {result['duplicates_discarded']} duplicates suppressed")
PY
"$root/local-compose" logs --no-color consumer-1 consumer-2 consumer-3 > "$root/artifacts/$RUN_ID/consumers.log"
printf 'Local recovery artifacts: %s\n' "$root/artifacts/$RUN_ID"
