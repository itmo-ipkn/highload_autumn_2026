"""Shared runtime helpers: env config, structured JSON logging, SIGTERM flag.

Single-process style: every pod prints one JSON object per line to stdout so
`docker logs` shows machine-readable events (producer sends, consumer attempts,
poison routing, rebalance) as evidence for the experiments.
"""
from __future__ import annotations

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

TERM = False
POD_ID = os.environ.get('POD_ID', 'unknown')


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, str(default))
    try:
        return int(raw)
    except ValueError:
        raise SystemExit(f'{name} must be an integer, got {raw!r}')


def env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, str(default))
    try:
        return float(raw)
    except ValueError:
        raise SystemExit(f'{name} must be a number, got {raw!r}')


def env_str(name: str, default: str) -> str:
    return os.environ.get(name, default)


def log(event: str, **fields) -> None:
    record = {
        'ts': datetime.now(timezone.utc).isoformat(timespec='milliseconds'),
        'pod': POD_ID,
        'event': event,
    }
    record.update(fields)
    print(json.dumps(record, ensure_ascii=False), flush=True)


def install_sigterm_handler(on_term=None) -> None:
    """SIGTERM sets the global TERM flag; callers check it between units of work.

    librdkafka callbacks cannot run inside a signal handler, so producers finish
    the current send loop, then flush() before exit (HW requirement).
    """
    def handler(signum, frame):
        globals()['TERM'] = True
        if on_term is not None:
            on_term()
    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)


def utc_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec='milliseconds')


def jitter_sleep(deadline: float) -> None:
    remaining = deadline - time.monotonic()
    if remaining > 0:
        time.sleep(remaining)
