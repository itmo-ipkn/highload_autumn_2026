from __future__ import annotations

import logging
import os
import signal
import sqlite3
import sys
import time

from kafka import ConsumerRebalanceListener, KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata, TopicPartition

from .event import Event

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

MAX_ATTEMPTS = 3


class ExitFunc(Exception):
    pass

# TODO: перенести в файл
SINK_SQL = """
CREATE TABLE IF NOT EXISTS sink (
    user_id INTEGER,
    seq INTEGER,
    event_id TEXT PRIMARY KEY,
    consumer_pod INTEGER,
    topic TEXT,
    partition INTEGER,
    offset INTEGER
);
CREATE TABLE IF NOT EXISTS stats (
    pod INTEGER PRIMARY KEY,
    processed INTEGER,
    duplicates INTEGER,
    dlt INTEGER
);
"""


def open_sink(path: str, pod: int) -> sqlite3.Connection:
    """ isolation_level=None чтобы писать сразу тремя подами """
    while True:
        conn = sqlite3.connect(path, isolation_level=None)
        try:
            conn.executescript(SINK_SQL)
            conn.execute("INSERT OR IGNORE INTO stats VALUES (?, 0, 0, 0)", (pod,))
            return conn
        except sqlite3.OperationalError as exc:
            conn.close()
            logging.info("pod %d waiting for the sink: %s", pod, exc)
            time.sleep(2)


def execute(
    conn: sqlite3.Connection, pod: int, sql: str, params: tuple
) -> sqlite3.Cursor:
    while True:
        try:
            return conn.execute(sql, params)
        except sqlite3.OperationalError as exc:
            logging.warning("pod %d sink busy: %s", pod, exc)
            time.sleep(1)


def write_stats(conn: sqlite3.Connection, pod: int, counters: dict) -> None:
    execute(
        conn,
        pod,
        "INSERT OR REPLACE INTO stats VALUES (?, ?, ?, ?)",
        (pod, counters["processed"], counters["duplicates"], counters["dlt"]),
    )


def process(
    conn: sqlite3.Connection,
    event: Event,
    pod: int,
    tp: TopicPartition,
    offset: int,
    delay_ms: int,
) -> bool:
    """ False, если дублируется ключ """
    time.sleep(delay_ms / 1000)
    cur = execute(
        conn,
        pod,
        "INSERT OR IGNORE INTO sink VALUES (?, ?, ?, ?, ?, ?, ?)",
        (event.user_id, event.seq, event.event_id, pod, tp.topic, tp.partition, offset),
    )
    return cur.rowcount == 1


def send_to_dlt(producer: KafkaProducer, dlt_topic: str, msg, reason: str) -> None:

    headers = [
        ("error", reason.encode()),
        ("orig_topic", msg.topic.encode()),
        ("orig_partition", str(msg.partition).encode()),
        ("orig_offset", str(msg.offset).encode()),
    ]
    producer.send(dlt_topic, value=msg.value, key=msg.key, headers=headers)
    producer.flush(timeout=10)


def commit(consumer: KafkaConsumer, tp: TopicPartition, next_offset: int) -> None:
    consumer.commit(offsets={tp: OffsetAndMetadata(next_offset)})


def main() -> None:
    pod = int(os.environ["POD_INDEX"])
    hosts = os.environ.get("KAFKA_HOSTS", "broker-1:9092,broker-2:9092,broker-3:9092")
    topic = os.environ.get("TOPIC", "events")
    dlt_topic = os.environ.get("DLT_TOPIC", "events.dlt")
    group = os.environ.get("GROUP", "events-processor")
    delay_ms = int(os.environ.get("PROCESSING_DELAY_MS", "50"))
    sink_path = os.environ.get("SINK", "/data/sink.sqlite")

    conn = open_sink(sink_path, pod)
    producer = KafkaProducer(bootstrap_servers=hosts)
    consumer = KafkaConsumer(
        group_id=group,
        bootstrap_servers=hosts,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=-1,
    )

    written: dict[TopicPartition, int] = {}

    def on_commit(offsets: dict, response: object) -> None:
        """A rebalance commit that failed is loud, but never fatal."""
        if isinstance(response, Exception):
            logging.error("pod %d rebalance commit failed: %s", pod, response)
        else:
            logging.info("pod %d rebalance commit ok: %s", pod, sorted(offsets))

    class Rebalance(ConsumerRebalanceListener):

        def on_partitions_revoked(self, revoked):
            offsets = {
                tp: OffsetAndMetadata(written[tp]) for tp in revoked if tp in written
            }
            if offsets:
                consumer.commit_async(offsets, callback=on_commit)

        def on_partitions_lost(self, lost):
            logging.warning("pod %d evicted from the group: %s", pod, sorted(lost))

        def on_partitions_assigned(self, assigned):
            logging.info("pod %d assigned: %s", pod, sorted(assigned))

    consumer.subscribe(topics=[topic], listener=Rebalance())

    counters = {"processed": 0, "duplicates": 0, "dlt": 0}
    consumed = poison = 0
    started = time.time()
    exit_code = 0

    def on_sigterm(signum: int, frame) -> None:
        raise ExitFunc()

    signal.signal(signal.SIGTERM, on_sigterm)

    try:
        while True:
            for msg in consumer:
                consumed += 1
                tp = TopicPartition(msg.topic, msg.partition)
                reason = None
                for attempt in range(1, MAX_ATTEMPTS + 1):
                    try:
                        event = Event.from_json(msg.value)
                        if not process(conn, event, pod, tp, msg.offset, delay_ms):
                            counters["duplicates"] += 1
                        counters["processed"] += 1
                        reason = None
                        break
                    except ValueError as exc:
                        reason = f"{exc} (attempt {attempt}/{MAX_ATTEMPTS})"
                        logging.warning(
                            "pod %d attempt %d/%d for message at offset %d failed: %s",
                            pod,
                            attempt,
                            MAX_ATTEMPTS,
                            msg.offset,
                            exc,
                        )
                if reason:
                    send_to_dlt(producer, dlt_topic, msg, reason)
                    counters["dlt"] += 1
                    poison += 1

                written[tp] = msg.offset + 1
                commit(consumer, tp, written[tp])
                write_stats(conn, pod, counters)
                if consumed % 200 == 0:
                    logging.info(
                        "pod %d consumed=%d poison=%d processed=%d dup=%d dlt=%d (%.1f ev/s)",
                        pod,
                        consumed,
                        poison,
                        counters["processed"],
                        counters["duplicates"],
                        counters["dlt"],
                        consumed / (time.time() - started),
                    )
    except ExitFunc:
        logging.info("pod %d got SIGTERM, committing and closing", pod)
    except KafkaError as exc:
        logging.error("pod %d consumer failed: %s", pod, exc)
        exit_code = 1
    finally:
        write_stats(conn, pod, counters)
        for tp, next_offset in written.items():
            try:
                commit(consumer, tp, next_offset)
            except KafkaError as exc:
                logging.error("pod %d final commit failed on %s: %s", pod, tp, exc)
        conn.close()
        try:
            consumer.close()
        except KafkaError as exc:
            logging.error("pod %d close failed: %s", pod, exc)
        producer.close()
        logging.info(
            "pod %d stopped: consumed=%d processed=%d duplicates=%d dlt=%d",
            pod,
            consumed,
            counters["processed"],
            counters["duplicates"],
            counters["dlt"],
        )

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
