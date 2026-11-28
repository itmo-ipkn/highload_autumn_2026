import json
import logging
import random
import signal
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, start_http_server

from config import ProducerSettings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

settings = ProducerSettings()

EVENT_TYPES = ["page_view"] * 7 + ["add_to_cart"] * 2 + ["purchase"]

EVENTS = Counter("producer_events_total", "Delivered events", ["pod", "type"])
ERRORS = Counter("producer_delivery_errors_total", "Failed deliveries", ["pod"])
INFLIGHT = Gauge("producer_inflight_messages", "Messages waiting for delivery report", ["pod"])

running = True


def stop(signum, _frame):
    global running
    log.info("signal %s received, stopping", signum)
    running = False


def on_delivery(err, msg):
    """Вызывается из poll()/flush() для каждого сообщения: ошибки не теряются молча."""
    if err is not None:
        ERRORS.labels(settings.POD_ID).inc()
        log.error("delivery failed: key=%s error=%s", msg.key(), err)
        return
    event_type = dict(msg.headers() or []).get("type", b"?").decode()
    EVENTS.labels(settings.POD_ID, event_type).inc()


def make_event(user_id: str, seq: int) -> dict:
    event_type = random.choice(EVENT_TYPES)
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "type": event_type,
        "seq": seq,
        "produced_at": datetime.now(timezone.utc).isoformat(),
        "payload": {
            "producer": settings.POD_ID,
            "poison": random.random() < settings.POISON_RATE,
            "item_id": random.randint(1, 1000),
        },
    }


def main():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    start_http_server(8000)

    # id запуска: после рестарта пода seq начинается с 1 уже для новых user_id
    run_id = uuid.uuid4().hex[:4]
    users = [f"{settings.POD_ID}-{run_id}-u{i:03d}" for i in range(settings.USERS_PER_PRODUCER)]
    seqs = dict.fromkeys(users, 0)

    producer = Producer({
        "bootstrap.servers": settings.BOOTSTRAP_SERVERS,
        "client.id": settings.POD_ID,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 20,
    })
    log.info("started: pod=%s run=%s key_mode=%s rate=%s/s users=%d",
             settings.POD_ID, run_id, settings.KEY_MODE, settings.EVENTS_PER_SEC, len(users))

    interval = 1.0 / settings.EVENTS_PER_SEC
    next_at = time.monotonic()
    while running:
        user_id = random.choice(users)
        event = make_event(user_id, seqs[user_id] + 1)
        key = user_id if settings.KEY_MODE == "user_id" else None
        try:
            producer.produce(
                settings.TOPIC,
                key=key,
                value=json.dumps(event).encode(),
                headers=[("type", event["type"].encode())],
                on_delivery=on_delivery,
            )
        except BufferError:
            # локальная очередь полна (брокеры недоступны): seq не увеличиваем, ждём и повторяем
            log.warning("local queue is full (%d messages), waiting", len(producer))
            producer.poll(1.0)
            continue
        seqs[user_id] += 1

        producer.poll(0)
        INFLIGHT.labels(settings.POD_ID).set(len(producer))
        next_at += interval
        delay = next_at - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        else:
            next_at = time.monotonic()

    # SIGTERM: досылаем всё, что осталось в буфере
    log.info("flushing %d buffered messages", len(producer))
    left = producer.flush(settings.FLUSH_TIMEOUT_S)
    if left:
        log.error("%d messages were NOT delivered before shutdown", left)
    else:
        log.info("all buffered messages delivered, bye")


if __name__ == "__main__":
    main()
