"""Event producer pod.

* key = user_id (KEY_MODE=user_id) or no key (KEY_MODE=none, experiment 3)
* acks=all + enable.idempotence=true
* every delivery error is logged and counted (never dropped silently)
* on SIGTERM the local buffer is flushed before exit
"""
import json
import random
import signal
import time
import uuid

from confluent_kafka import KafkaError, Producer
from prometheus_client import Counter, Gauge, start_http_server

from common import encode, env, make_event, setup_logging

POD_ID = env("POD_ID")
BOOTSTRAP = env("KAFKA_BOOTSTRAP")
TOPIC = env("TOPIC", "events")
RATE = env("RATE_PER_SEC", 30, float)
USERS_PER_POD = env("USERS_PER_POD", 10, int)
POISON_RATE = env("POISON_RATE", 0.02, float)
KEY_MODE = env("KEY_MODE", "user_id")
if KEY_MODE not in ("user_id", "none"):
    raise SystemExit("KEY_MODE must be user_id|none")

log = setup_logging(POD_ID)

M_SENT = Counter("producer_events_sent_total", "produce() calls", ["pod"])
M_ACKED = Counter("producer_events_acked_total", "events acknowledged by Kafka", ["pod", "partition"])
M_ERR = Counter("producer_delivery_errors_total", "failed deliveries", ["pod", "error"])
M_QUEUE = Gauge("producer_queue_messages", "messages buffered in librdkafka (not yet acked)", ["pod"])
M_BROKER_UP = Gauge("producer_broker_up", "broker connection state seen by producer", ["pod", "broker"])
M_RTT = Gauge("producer_broker_rtt_ms", "avg broker RTT", ["pod", "broker"])


def on_stats(raw: str):
    s = json.loads(raw)
    M_QUEUE.labels(POD_ID).set(s.get("msg_cnt", 0))
    for b in s.get("brokers", {}).values():
        if b.get("nodeid", -1) < 0:  # bootstrap pseudo-brokers
            continue
        name = b["name"].split("/")[0]
        M_BROKER_UP.labels(POD_ID, name).set(1 if b.get("state") == "UP" else 0)
        M_RTT.labels(POD_ID, name).set(b.get("rtt", {}).get("avg", 0) / 1000)


def on_error(err: KafkaError):
    # client-level errors (all brokers down, fatal idempotence errors, ...)
    log.error("client error: %s (fatal=%s)", err, err.fatal())


producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "client.id": POD_ID,
    "acks": "all",
    "enable.idempotence": True,          # no duplicates / reordering on internal retries
    "linger.ms": 5,
    "compression.type": "lz4",
    # how long a message may wait for an ack (incl. retries) before it is failed.
    # 5 minutes: survives the broker outage in experiment 2 without losing events.
    "message.timeout.ms": 300_000,
    "statistics.interval.ms": 2000,
    "stats_cb": on_stats,
    "error_cb": on_error,
})

running = True


def stop(signum, _frame):
    global running
    log.info("got signal %s, stopping", signal.Signals(signum).name)
    running = False


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)


def delivery_cb(err, msg):
    if err is not None:
        ev = json.loads(msg.value())
        M_ERR.labels(POD_ID, err.name()).inc()
        log.error("DELIVERY FAILED user=%s seq=%s event=%s: %s",
                  ev["user_id"], ev["seq"], ev["event_id"], err)
    else:
        M_ACKED.labels(POD_ID, str(msg.partition())).inc()


def main():
    start_http_server(8000)
    # a fresh "run" id per start: this pod is the only owner of these user_ids,
    # and a restart never reuses a user whose seq counter was lost
    run = uuid.uuid4().hex[:4]
    users = [f"{POD_ID.replace('producer-', 'p')}-{run}-u{i:02d}" for i in range(USERS_PER_POD)]
    seq = {u: 0 for u in users}
    log.info("start: rate=%s/s users=%d key_mode=%s poison_rate=%s users=%s..%s",
             RATE, len(users), KEY_MODE, POISON_RATE, users[0], users[-1])

    interval = 1.0 / RATE
    next_t = time.monotonic()
    sent = 0
    last_report = time.monotonic()
    while running:
        user = random.choice(users)
        seq[user] += 1
        ev = make_event(user, seq[user], POISON_RATE)
        key = user.encode() if KEY_MODE == "user_id" else None
        while True:
            try:
                producer.produce(TOPIC, key=key, value=encode(ev), on_delivery=delivery_cb)
                break
            except BufferError:
                # local queue full (e.g. all replicas unavailable) - wait, do not drop
                log.warning("local queue full, waiting")
                producer.poll(0.5)
        sent += 1
        M_SENT.labels(POD_ID).inc()
        producer.poll(0)

        if time.monotonic() - last_report > 10:
            log.info("sent=%d buffered=%d", sent, len(producer))
            last_report = time.monotonic()

        next_t += interval
        delay = next_t - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        else:
            next_t = time.monotonic()

    log.info("flushing %d buffered messages...", len(producer))
    remaining = producer.flush(60)
    if remaining:
        log.error("%d messages were NOT delivered before shutdown", remaining)
    else:
        log.info("flush done, all %d events acknowledged; last seq per user: %s", sent, seq)


if __name__ == "__main__":
    main()
