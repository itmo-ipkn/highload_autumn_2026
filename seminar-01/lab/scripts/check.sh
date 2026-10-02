#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -f .env ]] || { echo 'Copy env.example to .env and run python3 scripts/configure.py'; exit 1; }
docker compose config --quiet
curl -fsS --max-time 5 http://127.0.0.1:8080/healthz
printf '\n'
curl -fsS --max-time 5 http://127.0.0.1:8080/api/work
printf '\n'
curl -fsS --max-time 5 http://127.0.0.1:9090/-/ready
curl -fsS --max-time 5 http://127.0.0.1:3000/api/health
printf '\n'
curl -fsS --max-time 5 http://127.0.0.1:9090/api/v1/targets | python3 -c '
import sys,json
v=json.load(sys.stdin)["data"]["activeTargets"]
for x in v: print(x["labels"].get("job"), x["health"], x.get("lastError",""))
assert any(x["labels"].get("job")=="app" and x["health"]=="up" for x in v), "app target is not UP"
'
echo 'Readiness OK. node DOWN is expected without the Linux --profile host.'
