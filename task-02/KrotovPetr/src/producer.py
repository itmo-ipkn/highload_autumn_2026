"""Finite, rate-limited producer. A run cannot silently restart its sequences."""
import json
import os
import time

from confluent_kafka import Producer

from src.config import Config, log, setup_logging, stop_event
from src.events import make_event
from src.sink import claim_producer, finish_producer


def main():
    setup_logging()
    cfg = Config.load()
    producer_id = int(os.environ["PRODUCER_ID"])
    if producer_id not in (1, 2, 3):
        raise ValueError("PRODUCER_ID must be 1, 2 or 3")
    linger_ms = int(os.getenv("PRODUCER_LINGER_MS", "5"))
    if not 0 <= linger_ms <= 5000:
        raise ValueError("PRODUCER_LINGER_MS must be between 0 and 5000")
    stop = stop_event()
    producer = Producer({
        "bootstrap.servers": cfg.brokers,
        "client.id": f"producer-{producer_id}",
        "acks": "all", "enable.idempotence": True,
        "delivery.timeout.ms": 120000,
        "request.timeout.ms": 10000,
        "linger.ms": linger_ms,
        # Null keys must spread over partitions during the comparison run.
        "sticky.partitioning.linger.ms": 0,
    })
    claim_producer(cfg, producer_id)
    generated = acknowledged = errors = 0

    def delivered(err, msg):
        nonlocal acknowledged, errors
        if err is not None:
            errors += 1
            log("delivery_error", producer=producer_id, key=msg.key(), error=str(err))
        else:
            acknowledged += 1
            if acknowledged % 100 == 0:
                log("delivery_progress", producer=producer_id, acknowledged=acknowledged,
                    partition=msg.partition(), offset=msg.offset())

    log("producer_started", producer=producer_id, run_id=cfg.run_id,
        expected=cfg.users * cfg.events, key_mode=cfg.key_mode)
    status = "failed"
    try:
        next_send = time.monotonic()
        last_report = next_send
        for seq in range(1, cfg.events + 1):
            for number in range(1, cfg.users + 1):
                producer.poll(0)
                if time.monotonic() - last_report >= 1:
                    log("producer_progress", producer=producer_id, generated=generated,
                        acknowledged=acknowledged, queued=len(producer), errors=errors)
                    last_report = time.monotonic()
                if stop.is_set() or errors:
                    break
                event = make_event(cfg, producer_id, number, seq)
                while not stop.is_set():
                    try:
                        producer.produce(
                            "events", value=json.dumps(event).encode(),
                            key=event["user_id"].encode() if cfg.key_mode == "user_id" else None,
                            on_delivery=delivered,
                        )
                        generated += 1
                        break
                    except BufferError:
                        log("producer_backpressure", producer=producer_id)
                        producer.poll(0.1)
                next_send = max(next_send + 1 / cfg.rate, time.monotonic())
                stop.wait(max(0, next_send - time.monotonic()))
            if stop.is_set() or errors:
                break
        log("producer_flushing", producer=producer_id, generated=generated, queued=len(producer))
        remaining = producer.flush(125)
        if remaining:
            errors += remaining
            log("flush_incomplete", producer=producer_id, remaining=remaining)
        if errors or acknowledged != generated:
            status = "failed"
        elif generated == cfg.users * cfg.events:
            status = "complete"
        else:
            status = "interrupted"
    finally:
        finish_producer(cfg, producer_id, status, acknowledged, errors)
        log("producer_finished", producer=producer_id, status=status,
            generated=generated, acknowledged=acknowledged, errors=errors)
    return 0 if status == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
