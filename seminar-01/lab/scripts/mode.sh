#!/usr/bin/env bash
# One authenticated POST. No service restart, no load, no counter reset.
set -euo pipefail
cd "$(dirname "$0")/.."
MODE=${1:-normal}
case "$MODE" in normal|tail|slow|errors) ;; *) echo 'Use normal|tail|slow|errors' >&2; exit 2;; esac
TOKEN=$(python3 - <<'PYTOKEN'
from pathlib import Path
import re
values = dict(line.split('=', 1) for line in Path('.env').read_text().splitlines() if line and not line.startswith('#'))
v = values['LAB_ADMIN_TOKEN']
if not re.fullmatch(r'[0-9a-f]{32,128}', v):
    raise SystemExit('Run python3 scripts/configure.py')
print(v)
PYTOKEN
)
# Feed the header through stdin: no token in argv/ps or shell trace. Never use bash -x.
printf 'header = "X-Lab-Token: %s"\n' "$TOKEN" | \
  curl --config - --fail-with-body --silent --show-error --max-time 5 \
  -H 'Content-Type: application/json' -d "{\"mode\":\"$MODE\"}" \
  http://127.0.0.1:8081/admin/mode
unset TOKEN
printf '\n'
