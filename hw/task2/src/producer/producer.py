#!/usr/bin/env python3

from __future__ import annotations

import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import KafkaException, Producer

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] [producer-%(producer_id)s] %(message)s",
    stream=sys.stdout,
)


class _DefaultProducerId(logging.Filter):

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "producer_id"):
            record.producer_id = "shared"
        return True


for _handler in logging.getLogger().handlers:
    _handler.addFilter(_DefaultProducerId())


class ProducerLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg: str, kwargs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        extra = kwargs.setdefault("extra", {})
        extra.setdefault("producer_id", self.extra["producer_id"])
        return msg, kwargs


PRODUCER_ID = int(os.getenv("PRODUCER_ID", "1"))
KEY_MODE = os.getenv("KEY_MODE", "user_id").strip().lower()
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "5"))
EVENTS_TOPIC = os.getenv("EVENTS_TOPIC", "events")
BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092",
)
POISON_EVENT_RATIO = float(os.getenv("POISON_EVENT_RATIO", "0.02"))
USERS_PER_PRODUCER = int(os.getenv("USERS_PER_PRODUCER", "10"))
STATE_DIR = Path(os.getenv("STATE_DIR", "/data"))
STATE_FILE = STATE_DIR / f"producer-{PRODUCER_ID}.json"
FLUSH_TIMEOUT_SECONDS = float(os.getenv("FLUSH_TIMEOUT_SECONDS", "120"))

if PRODUCER_ID < 1:
    raise ValueError("PRODUCER_ID must be a positive integer")
if KEY_MODE not in {"user_id", "none"}:
    raise ValueError("KEY_MODE must be either 'user_id' or 'none'")
if EVENTS_PER_SECOND <= 0:
    raise ValueError("EVENTS_PER_SECOND must be greater than 0")
if not 0.0 <= POISON_EVENT_RATIO <= 1.0:
    raise ValueError("POISON_EVENT_RATIO must be between 0 and 1")
if USERS_PER_PRODUCER < 1:
    raise ValueError("USERS_PER_PRODUCER must be at least 1")

logger = ProducerLoggerAdapter(logging.getLogger(__name__), {"producer_id": PRODUCER_ID})
shutdown_event = False


USER_IDS = [f"user_{PRODUCER_ID}_{i}" for i in range(1, USERS_PER_PRODUCER + 1)]
user_sequences: dict[str, int] = {user_id: 0 for user_id in USER_IDS}
EVENT_TYPES = ("page_view", "add_to_cart", "purchase")
stats = {
    "generated": 0,
    "enqueued": 0,
    "delivered": 0,
    "delivery_errors": 0,
    "enqueue_errors": 0,
}


def handle_shutdown(signum: int, _frame: Any) -> None:
    global shutdown_event
    logger.info("Received signal %s; finishing current event and flushing", signum)
    shutdown_event = True


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def load_state() -> None:
    if not STATE_FILE.exists():
        logger.info("No state file at %s; starting sequences from 1", STATE_FILE)
        return

    try:
        state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Cannot read producer state {STATE_FILE}: {exc}") from exc

    stored_producer_id = state.get("producer_id")
    if stored_producer_id is not None and int(stored_producer_id) != PRODUCER_ID:
        raise RuntimeError(
            f"State file {STATE_FILE} belongs to producer {stored_producer_id}, "
            f"but PRODUCER_ID={PRODUCER_ID}"
        )

    sequences = state.get("sequences", {})
    if not isinstance(sequences, dict):
        raise RuntimeError(f"Malformed state file {STATE_FILE}: 'sequences' is not an object")

    for user_id in USER_IDS:
        value = int(sequences.get(user_id, 0))
        if value < 0:
            raise RuntimeError(f"Malformed seq for {user_id} in {STATE_FILE}: {value}")
        user_sequences[user_id] = value

    logger.info(
        "Restored sequences from %s: min=%s max=%s",
        STATE_FILE,
        min(user_sequences.values()),
        max(user_sequences.values()),
    )


def persist_state() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    tmp_file = STATE_FILE.with_suffix(".json.tmp")
    payload = {
        "producer_id": PRODUCER_ID,
        "updated_at": utc_now_iso(),
        "sequences": user_sequences,
    }
    try:
        with tmp_file.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_file, STATE_FILE)
    except OSError:

        logger.exception("Could not persist producer state to %s", STATE_FILE)
        raise


def create_event(user_id: str, seq: int) -> dict[str, Any]:
    now = time.time()
    poison = random.random() < POISON_EVENT_RATIO
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "type": random.choice(EVENT_TYPES),
        "seq": seq,
        "produced_at": utc_now_iso(),
        "payload": {
            "producer_id": PRODUCER_ID,
            "key_mode": KEY_MODE,
            "poison": poison,
            "produced_at_unix": now,
        },
    }


def delivery_report(err: Any, message: Any, event_id: str) -> None:
    if err is not None:
        stats["delivery_errors"] += 1
        logger.error(
            "Delivery failed: event_id=%s topic=%s partition=%s key=%r error=%s",
            event_id,
            message.topic(),
            message.partition(),
            message.key(),
            err,
        )
        return

    stats["delivered"] += 1
    logger.debug(
        "Delivered event_id=%s to %s[%s] offset=%s",
        event_id,
        message.topic(),
        message.partition(),
        message.offset(),
    )


def build_producer() -> Producer:


    config = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": f"event-producer-{PRODUCER_ID}",
        "acks": "all",
        "enable.idempotence": True,
        "retries": 2_147_483_647,
        "max.in.flight.requests.per.connection": 5,
        "linger.ms": 50,
        "batch.size": 16_384,


        "message.timeout.ms": 300_000,
    }
    return Producer(config)


def enqueue(producer: Producer, user_id: str) -> bool:
    seq = user_sequences[user_id] + 1
    event = create_event(user_id, seq)
    key = user_id if KEY_MODE == "user_id" else None
    value = json.dumps(event, separators=(",", ":"))
    stats["generated"] += 1

    def on_delivery(err: Any, message: Any) -> None:
        delivery_report(err, message, event["event_id"])

    try:
        producer.produce(
            topic=EVENTS_TOPIC,
            key=key,
            value=value,
            on_delivery=on_delivery,
        )
    except BufferError:

        producer.poll(0.5)
        try:
            producer.produce(
                topic=EVENTS_TOPIC,
                key=key,
                value=value,
                on_delivery=on_delivery,
            )
        except (BufferError, KafkaException) as exc:
            stats["enqueue_errors"] += 1
            logger.error("Could not enqueue event_id=%s: %s", event["event_id"], exc)
            return False
    except KafkaException as exc:
        stats["enqueue_errors"] += 1
        logger.error("Could not enqueue event_id=%s: %s", event["event_id"], exc)
        return False



    user_sequences[user_id] = seq
    persist_state()
    stats["enqueued"] += 1
    producer.poll(0)
    return True


def main() -> None:
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    logger.info(
        "Starting bootstrap=%s topic=%s key_mode=%s rate=%s events/sec users=%s poison_ratio=%s",
        BOOTSTRAP_SERVERS,
        EVENTS_TOPIC,
        KEY_MODE,
        EVENTS_PER_SECOND,
        len(USER_IDS),
        POISON_EVENT_RATIO,
    )

    load_state()
    producer = build_producer()
    interval = 1.0 / EVENTS_PER_SECOND
    next_event_at = time.monotonic()
    remaining = 0

    try:
        while not shutdown_event:
            user_id = random.choice(USER_IDS)
            enqueue(producer, user_id)

            next_event_at += interval
            delay = next_event_at - time.monotonic()
            if delay < 0:


                next_event_at = time.monotonic()
                delay = 0
            if shutdown_event:
                break
            time.sleep(min(delay, 0.5))
    finally:
        logger.info("Flushing buffered messages before shutdown...")
        remaining = producer.flush(FLUSH_TIMEOUT_SECONDS)
        if remaining:
            logger.error(
                "Shutdown flush timed out: %s message(s) remain queued and may be lost",
                remaining,
            )
        persist_state()
        logger.info(
            "Stopped. generated=%s enqueued=%s delivered=%s delivery_errors=%s "
            "enqueue_errors=%s queued_after_flush=%s",
            stats["generated"],
            stats["enqueued"],
            stats["delivered"],
            stats["delivery_errors"],
            stats["enqueue_errors"],
            remaining,
        )


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Producer terminated due to an unexpected error")
        raise
