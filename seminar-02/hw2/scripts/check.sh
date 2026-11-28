#!/usr/bin/env bash
# Static checks + offline unit tests; then readiness of the running stand.
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -f .env ]] || { echo 'Copy .env.example to .env and run python scripts/configure.py'; exit 1; }
# Prefer the project venv when present (Git Bash on Windows resolves `python` to
# the system interpreter, which does not have pytest installed).
if [[ -x .venv/Scripts/python.exe ]]; then PY=.venv/Scripts/python.exe
elif [[ -x .venv/bin/python ]]; then PY=.venv/bin/python
else PY=${VENV_PY:-python}; fi
docker compose config --quiet
"$PY" -m pytest -q tests
curl -fsS --max-time 5 http://127.0.0.1:9090/-/ready
curl -fsS --max-time 5 http://127.0.0.1:3000/api/health
printf '\n'
curl -fsS --max-time 5 http://127.0.0.1:9090/api/v1/targets | "$PY" -c '
import sys,json
v=json.load(sys.stdin)["data"]["activeTargets"]
for x in v: print(x["labels"].get("job"), x["health"], x.get("lastError",""))
assert any(x["labels"].get("job")=="kafka-exporter" and x["health"]=="up" for x in v), "kafka-exporter target is not UP"
'
echo 'Readiness OK.'
