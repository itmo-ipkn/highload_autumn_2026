import json
import logging
import signal
import sys
import time

import psycopg2
from confluent_kafka import Consumer, Producer, TopicPartition
from prometheus_client import start_http_server, Counter, Histogram, Gauge

from common.config import (KAFKA_BOOTSTRAP, TOPIC_EVENTS, TOPIC_DLT, PG_DSN,
                           POD_ID, PROCESSING_DELAY_MS, POISON_MAX_ATTEMPTS)
from common.events import Event

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s | %(message)s")
log = logging.getLogger("consumer")

PROCESSED = Counter("consumer_events_processed_total", "Events written to sink", ["pod"])
DUPLICATES = Counter("consumer_duplicates_dropped_total", "Duplicates suppressed", ["pod"])
TO_DLT = Counter("consumer_poison_to_dlt_total", "Poison events sent to DLT", ["pod"])
POISON_RETRIES = Counter("consumer_poison_retries_total", "Poison processing retries", ["pod"])
PROCESS_TIME = Histogram("consumer_processing_seconds", "Processing time", ["pod"],
                         buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0))
ASSIGNED = Gauge("consumer_partitions_assigned", "Assigned partitions", ["pod"])

stop = False
consumer: Consumer = None
conn = None
dlt_producer: Producer = None


class PoisonError(Exception):
    """Событие, которое при любой попытке обработки приводит к ошибке."""


def handle_signal(signum, frame):
    global stop
    stop = True


def connect_pg():
    for attempt in range(1, 31):
        try:
            c = psycopg2.connect(PG_DSN)
            c.autocommit = False
            return c
        except psycopg2.Error as e:
            log.warning("postgres not ready (%s), retry %d/30", e, attempt)
            time.sleep(2)
    raise RuntimeError("postgres unavailable")


def bump_attempts(event_id: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO poison_attempts(event_id, attempts, updated_at)
               VALUES (%s, 1, now())
               ON CONFLICT (event_id)
               DO UPDATE SET attempts = poison_attempts.attempts + 1, updated_at = now()
               RETURNING attempts""",
            (event_id,),
        )
        attempts = cur.fetchone()[0]
    conn.commit()
    return attempts


def write_to_sink(ev, topic: str, partition: int, offset: int) -> bool:
    """Идемпотентная запись по event_id. Возвращает False, если это дубль."""
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO events(event_id, user_id, seq, type, produced_at,
                                  payload, processed_by, topic, partition, kafka_offset)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (event_id) DO NOTHING""",
            (ev.event_id, ev.user_id, ev.seq, ev.type, ev.produced_at,
             json.dumps(ev.payload), int(POD_ID), topic, partition, offset),
        )
        inserted = cur.rowcount == 1
        if not inserted:
            cur.execute("UPDATE metrics SET duplicates = duplicates + 1 WHERE id = 1")
            DUPLICATES.labels(POD_ID).inc()
    conn.commit()
    return inserted


def send_to_dlt(msg, event_id: str, reason: str, attempts: int):
    headers = list(msg.headers() or []) + [
        ("error_reason", reason.encode()),
        ("attempts", str(attempts).encode()),
        ("origin_event_id", event_id.encode()),
    ]
    dlt_producer.produce(
        TOPIC_DLT,
        key=msg.key(),
        value=msg.value(),
        headers=headers,
    )
    dlt_producer.flush(10)
    TO_DLT.labels(POD_ID).inc()
    log.warning("event_id=%s moved to %s after %d attempts: %s",
                event_id, TOPIC_DLT, attempts, reason)


def process_event(msg):
    try:
        ev = Event.from_json(msg.value().decode())
    except Exception as e:
        raise PoisonError(f"unparseable payload: {e}")

    with PROCESS_TIME.labels(POD_ID).time():
        time.sleep(PROCESSING_DELAY_MS / 1000.0)  # эмуляция работы
        if ev.is_poison():
            raise PoisonError("poison payload detected")
        write_to_sink(ev, msg.topic(), msg.partition(), msg.offset())
    PROCESSED.labels(POD_ID).inc()


def handle_message(msg):
    try:
        ev = Event.from_json(msg.value().decode())
        event_id = ev.event_id
    except Exception:
        event_id = f"raw-{msg.topic()}-{msg.partition()}-{msg.offset()}"

    while not stop:
        try:
            process_event(msg)
            consumer.commit(message=msg, asynchronous=False)  # at-least-once
            return
        except PoisonError as e:
            attempts = bump_attempts(event_id)
            if attempts >= POISON_MAX_ATTEMPTS:
                send_to_dlt(msg, event_id, str(e), attempts)
                consumer.commit(message=msg, asynchronous=False)
                return
            POISON_RETRIES.labels(POD_ID).inc()
            log.warning("poison event_id=%s attempt %d/%d failed: %s",
                        event_id, attempts, POISON_MAX_ATTEMPTS, e)
            time.sleep(min(0.5 * attempts, 2.0))
        except psycopg2.Error as e:
            # инфраструктурная ошибка — не считаем попытками, ждём восстановления
            log.error("sink error, will retry: %s", e)
            try:
                conn.rollback()
            except Exception:
                pass
            time.sleep(1.0)


def on_assign(consumer_, partitions):
    log.info("partitions assigned: %s",
             [(p.topic, p.partition, p.offset) for p in partitions])
    ASSIGNED.labels(POD_ID).set(len(partitions))


def on_revoke(consumer_, partitions):
    # при ребалансе фиксируем всё, что уже обработано
    log.info("partitions revoked, committing offsets: %s",
             [(p.topic, p.partition) for p in partitions])
    try:
        consumer_.commit(asynchronous=False)
    except Exception as e:
        log.warning("commit on revoke failed: %s", e)
    ASSIGNED.labels(POD_ID).set(0)


def main():
    global consumer, conn, dlt_producer
    start_http_server(8000)
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    conn = connect_pg()
    log.info("consumer pod=%s connected to sink", POD_ID)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "events-processor",
        "auto.offset.reset": "earliest",   # осознанный выбор: at-least-once, см. README
        "enable.auto.commit": False,       # оффсет коммитим только после записи в sink
        "enable.auto.offset.store": False,
        "max.poll.interval.ms": 300000,
    })

    dlt_producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "enable.idempotence": True,
        "acks": "all",
    })

    consumer.subscribe([TOPIC_EVENTS], on_assign=on_assign, on_revoke=on_revoke)
    log.info("consumer pod=%s joined group events-processor", POD_ID)

    try:
        while not stop:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("kafka error: %s", msg.error())
                continue
            handle_message(msg)
    finally:
        log.info("consumer pod=%s stopping...", POD_ID)
        try:
            consumer.commit(asynchronous=False)
        except Exception:
            pass
        consumer.close()
        conn.close()
        log.info("consumer pod=%s stopped", POD_ID)


if __name__ == "__main__":
    main()
