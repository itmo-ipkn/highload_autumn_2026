import logging
import os
import random
import signal
import time
import uuid

from confluent_kafka import Producer
from prometheus_client import start_http_server, Counter

from common.config import KAFKA_BOOTSTRAP, TOPIC_EVENTS, KEY_MODE
from common.events import Event, EVENT_TYPES

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s | %(message)s")
log = logging.getLogger("producer")

POD_ID = os.getenv("POD_ID", "0")
USERS_PER_POD = int(os.getenv("USERS_PER_POD", "50"))
EVENTS_PER_SEC = float(os.getenv("EVENTS_PER_SEC", "5"))
RUN_DURATION = float(os.getenv("RUN_DURATION", "0"))  # 0 = бесконечно
POISON_RATE = float(os.getenv("POISON_RATE", "0.02"))

EVENTS_SENT = Counter("producer_events_sent_total", "Events delivered (acked)", ["pod"])
SEND_ERRORS = Counter("producer_send_errors_total", "Delivery errors", ["pod"])

stop = False


def handle_signal(signum, frame):
    global stop
    stop = True
    log.info("signal %s received, graceful shutdown started", signum)


def main():
    start_http_server(8000)
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    # user_id генерируется ровно одним подом: у каждого пода свой диапазон пользователей
    pod_num = int(POD_ID)
    users = [f"user-{pod_num:02d}-{i:04d}" for i in range(1, USERS_PER_POD + 1)]
    seqs = {u: 0 for u in users}

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "enable.idempotence": True,   # идемпотентный продюсер
        "acks": "all",                # подтверждение всех ISR
        "linger.ms": 5,
        "retries": 2147483647,
    })

    def delivery_cb(err, msg):
        if err is not None:
            SEND_ERRORS.labels(POD_ID).inc()
            log.error("delivery failed: %s (key=%s, seq lost!)",
                      err, msg.key())
        else:
            EVENTS_SENT.labels(POD_ID).inc()

    interval = 1.0 / EVENTS_PER_SEC
    next_tick = time.monotonic()
    deadline = time.monotonic() + RUN_DURATION if RUN_DURATION > 0 else None
    total = 0

    log.info("producer pod=%s started: users=%d, rate=%.1f/s, key_mode=%s",
             POD_ID, len(users), EVENTS_PER_SEC, KEY_MODE)

    while not stop:
        user = random.choice(users)
        seqs[user] += 1
        poison = random.random() < POISON_RATE
        ev = Event(
            event_id=str(uuid.uuid4()),
            user_id=user,
            type=random.choice(EVENT_TYPES),
            seq=seqs[user],
            produced_at=__import__("datetime").datetime.now(
                __import__("datetime").timezone.utc).isoformat(),
            payload={"poison": True, "reason": "simulated bad record"} if poison
                    else {"action": "click", "value": round(random.random(), 4)},
        )
        key = ev.user_id.encode() if KEY_MODE == "user_id" else None
        headers = [("poison", b"true")] if poison else None

        while True:
            try:
                producer.produce(TOPIC_EVENTS, key=key,
                                 value=ev.to_json().encode(),
                                 headers=headers, on_delivery=delivery_cb)
                break
            except BufferError:
                producer.poll(0.1)

        producer.poll(0)
        total += 1
        next_tick += interval
        pause = next_tick - time.monotonic()
        if pause > 0:
            time.sleep(pause)
        else:
            next_tick = time.monotonic()
        if deadline and time.monotonic() >= deadline:
            break

    log.info("producer pod=%s stopping: flushing %d buffered messages...", POD_ID, total)
    remaining = producer.flush(30)
    if remaining:
        log.error("producer pod=%s: %d messages were NOT delivered on shutdown",
                  POD_ID, remaining)
    else:
        log.info("producer pod=%s: all buffered messages delivered, bye", POD_ID)


if __name__ == "__main__":
    main()
