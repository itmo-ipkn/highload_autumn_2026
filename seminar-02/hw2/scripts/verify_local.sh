#!/usr/bin/env bash
# Same verify CLI, run from the host venv against the EXTERNAL listeners.
# Use this when you want the CLI outside docker (read-only checks against the stand).
# Run it with the project venv (psycopg + confluent-kafka are installed there);
# a plain `python` on PATH may be the system interpreter, which is why activation
# appears not to work.
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -f .env ]] || { echo 'Copy .env.example to .env and run python scripts/configure.py'; exit 1; }
set -a
source .env
set +a
# Prefer the project venv when present (same convention as scripts/check.sh).
if [[ -x .venv/Scripts/python.exe ]]; then PY=.venv/Scripts/python.exe
elif [[ -x .venv/bin/python ]]; then PY=.venv/bin/python
else PY=${VENV_PY:-python}; fi
# Host CLI must connect through the EXTERNAL advertised listeners as 127.0.0.1:9094/9095/9096.
BOOTSTRAP_SERVERS=127.0.0.1:9094,127.0.0.1:9095,127.0.0.1:9096 \
PGHOST=127.0.0.1 PGPORT=5432 \
"$PY" verify/verify.py
