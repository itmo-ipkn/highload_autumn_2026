#!/usr/bin/env bash
# End a run so the pods exit and evidence can be collected.
# Finite RUN_DURATION -> pods exit on their own (producer deadline, consumer idle-exit).
# RUN_DURATION=0 (keep-running mode) -> nothing exits on its own, so the run is bounded
# by WINDOW seconds and then stopped with SIGTERM, which is the graceful path: producers
# flush, consumers commit processed offsets, exit codes stay 0.
# Used by run_load.sh and the experiment drivers.
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -f .env ]] || { echo 'Copy .env.example to .env and run python scripts/configure.py'; exit 2; }
set -a
source .env
set +a
WINDOW=${1:-120}
GRACE=20   # >= FLUSH_TIMEOUT_SECONDS so producers flush inside the grace period
DRAIN=15   # let consumers commit what producers buffered before their last flush
[[ "$WINDOW" =~ ^[0-9]{1,4}$ ]] || { echo 'WINDOW must be an integer number of seconds' >&2; exit 2; }
[[ "$RUN_DURATION" != 0 ]] && exit 0
[[ "$WINDOW" != 0 ]] || { echo 'WINDOW must be > 0 when RUN_DURATION=0' >&2; exit 2; }
sleep "$WINDOW"
# Producers first, then consumers: SIGTERM flushes producers and lets consumers
# commit processed offsets, so no gap is left uncommitted in front of verify.
docker compose stop -t "$GRACE" producer1 producer2 producer3 >/dev/null
sleep "$DRAIN"
docker compose stop -t "$GRACE" consumer1 consumer2 consumer3 >/dev/null
