#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
if [ "${1:-native}" = "docker" ]; then
  docker compose config --quiet
  docker compose run --rm client check
else
  .venv/bin/python tools/demo.py check
fi
