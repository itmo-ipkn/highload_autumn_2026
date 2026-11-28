from __future__ import annotations

import logging
import os
import random
import signal
import sys
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.partitioner.default import murmur2

from .event import EVENT_TYPES, Event

logging.basicConfig(
    level=logging.INFO, #logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s"
)

POISON_RATE = 0.02 #0.9


class ExitFunc(Exception): #Panic???
    pass


def main() -> None:

    pod_index = int(os.environ["POD_INDEX"])
    users_per_pod = int(os.environ.get("USERS_PER_POD", "50"))
    rate = float(os.environ.get("EVENTS_PER_SEC", "20"))
    key_mode = os.environ.get("KEY_MODE", "user_id")
    topic = os.environ.get("TOPIC", "events")
    hosts = os.environ.get("KAFKA_HOSTS", "broker-1:9092,broker-2:9092,broker-3:9092")

    users = range((pod_index - 1) * users_per_pod + 1, pod_index * users_per_pod + 1)
    seq_of = {user_id: 0 for user_id in users}

    producer = KafkaProducer(
        bootstrap_servers=hosts,
        acks=-1,  # acks=all
        enable_idempotence=True,  # enable.idempotence=true
        max_block_ms=10_000,
    )

    def next_event() -> tuple[Event, bool]:
        user_id = random.choice(users)
        seq_of[user_id] += 1
        event = Event(
            user_id=user_id,
            type=random.choice(EVENT_TYPES),
            seq=seq_of[user_id],
            payload={"pod": pod_index},
        )
        return event, random.random() < POISON_RATE

    def report_failure(exc: BaseException, event: Event, key: bytes | None) -> None:
        """2.6: a send error is never swallowed — log event content, key, partition."""
        partitions = len(producer.partitions_for(topic) or range(1))
        partition = (murmur2(key) & 0x7FFFFFFF) % partitions if key else "random"
        logging.error(
            "pod %d send failed: %s | key=%s partition=%s event=%s",
            pod_index,
            exc,
            key.decode() if key else None,
            partition,
            event.to_dict(),
        )

    def on_sigterm(signum: int, frame) -> None:
        raise ExitFunc()

    signal.signal(signal.SIGTERM, on_sigterm)

    produced = poison = 0
    interval = 1.0 / rate
    next_tick = time.time()
    started = next_tick
    try:
        while True:
            event, mismatched = next_event()
            raw = event.to_json_v2() if mismatched else event.to_json()
            key = str(event.user_id).encode() if key_mode == "user_id" else None
            future = producer.send(topic, value=raw, key=key)
            future.add_errback(lambda exc, e=event, k=key: report_failure(exc, e, k))
            produced += 1
            poison += int(mismatched)
            if produced % 200 == 0:
                logging.info(
                    "pod %d produced=%d poison=%d (%.1f ev/s)",
                    pod_index,
                    produced,
                    poison,
                    produced / (time.time() - started),
                )
            next_tick += interval
            delay = next_tick - time.time()
            if delay > 0:
                time.sleep(delay)
    except ExitFunc:
        logging.info("pod %d got SIGTERM, flushing buffered messages", pod_index)
    except (KafkaError, OSError) as exc:
        logging.error("pod %d producer failed: %s", pod_index, exc)
    finally:
        try:
            producer.flush(timeout=30)
        except KafkaError as exc:
            logging.error("pod %d flush failed: %s", pod_index, exc)
        producer.close()
        logging.info(
            "pod %d stopped: produced=%d poison=%d", pod_index, produced, poison
        )

    sys.exit(0)


if __name__ == "__main__":
    main()
