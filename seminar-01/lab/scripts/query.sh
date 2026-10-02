#!/usr/bin/env bash
# Read one PromQL expression and send an HTTP GET to Prometheus; optional Unix time.
set -euo pipefail
cd "$(dirname "$0")/.."
KEY=${1:-good}; AT=${2:-}
QUERY=$(python3 - "$KEY" <<'PYQUERY'
import json, sys
q=json.load(open('prometheus/queries.json'))
if sys.argv[1] not in q: raise SystemExit('Unknown query key: '+sys.argv[1])
print(q[sys.argv[1]])
PYQUERY
)
ARGS=(--data-urlencode "query=$QUERY")
if [[ -n "$AT" ]]; then ARGS+=(--data-urlencode "time=$AT"); fi
curl --fail --silent --show-error --max-time 10 -G http://127.0.0.1:9090/api/v1/query   "${ARGS[@]}" | python3 -c '
import json,sys
x=json.load(sys.stdin)
if x.get("status") != "success": raise SystemExit(json.dumps(x))
print(json.dumps(x,indent=2))
'
