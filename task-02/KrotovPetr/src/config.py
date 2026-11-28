"""Configuration shared by the three local application commands."""
from dataclasses import dataclass
import json
import logging
import os
import re
import signal
import threading
from datetime import datetime, timezone


@dataclass(frozen=True)
class Config:
    run_id: str
    users: int
    events: int
    rate: float
    poison_rate: float
    seed: int
    key_mode: str
    delay_ms: int
    before_commit_ms: int
    brokers: str

    @classmethod
    def load(cls):
        cfg = cls(
            run_id=os.getenv("RUN_ID", "baseline"),
            users=int(os.getenv("USERS_PER_PRODUCER", "10")),
            events=int(os.getenv("EVENTS_PER_USER", "100")),
            rate=float(os.getenv("RATE_PER_SECOND", "30")),
            poison_rate=float(os.getenv("POISON_RATE", "0.02")),
            seed=int(os.getenv("SEED", "42")),
            key_mode=os.getenv("KEY_MODE", "user_id"),
            delay_ms=int(os.getenv("PROCESSING_DELAY_MS", "10")),
            before_commit_ms=int(os.getenv("BEFORE_COMMIT_DELAY_MS", "0")),
            brokers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"),
        )
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", cfg.run_id):
            raise ValueError("RUN_ID must contain 1..64 letters, digits, '_' or '-'")
        if cfg.users < 1 or cfg.events < 1 or not 0 < cfg.rate < float("inf"):
            raise ValueError("User count, event count and rate must be positive and finite")
        if not 0 <= cfg.poison_rate <= 1 or min(cfg.delay_ms, cfg.before_commit_ms) < 0:
            raise ValueError("Invalid poison rate or delay")
        if cfg.key_mode not in {"user_id", "none"}:
            raise ValueError("KEY_MODE must be user_id or none")
        allowed = {f"kafka-{n}:9092" for n in range(1, 4)}
        if not cfg.brokers or not set(cfg.brokers.split(",")) <= allowed:
            raise ValueError("This lab accepts only its Compose Kafka addresses")
        return cfg


def setup_logging():
    logging.basicConfig(level=logging.INFO, format="%(message)s")


def log(action, **fields):
    logging.info(json.dumps({
        "time": datetime.now(timezone.utc).isoformat(), "action": action, **fields,
    }, ensure_ascii=False, default=str))


def stop_event():
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    return stop
