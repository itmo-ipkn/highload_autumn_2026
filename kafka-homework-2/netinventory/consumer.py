import json
import logging
import os
from time import monotonic

from confluent_kafka import Consumer, KafkaException, TopicPartition
from prometheus_client import Counter, start_http_server

from .events import UnknownDevice, check_device
from .settings import BOOTSTRAP, DEAD_LETTER_TOPIC, GROUP, TOPIC, configure_logging, database, kafka_producer, send, stop_event

RESULTS = Counter("netinventory_consumer_events", "Consumer processing outcomes", ("consumer", "outcome"))
ERRORS = Counter("netinventory_processing_errors", "Consumer processing failures", ("consumer", "reason"))


def save_event(connection, message, event, code, consumer_id):
    user_id = event["user_id"]
    with connection.transaction():
        # Keep the recorded write order meaningful even when one user spans several partitions.
        connection.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (user_id,))
        inserted = connection.execute(
            """
            INSERT INTO processed_events (
                event_id, user_id, mode, seq, event_type, device_code, consumer_id, topic, partition, kafka_offset
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
            """,
            (
                event["event_id"], user_id, event["payload"]["key_mode"], event["seq"],
                event["type"], code, consumer_id, message.topic(), message.partition(), message.offset(),
            ),
        ).fetchone()
        if inserted is None:
            connection.execute(
                """
                INSERT INTO duplicate_deliveries (event_id, count) VALUES (%s, 1)
                ON CONFLICT (event_id) DO UPDATE SET count = duplicate_deliveries.count + 1
                """,
                (event["event_id"],),
            )
    return inserted is not None


class ProcessedOffsets:
    def __init__(self):
        self.pending = {}
        self.first_read = set()

    def assign(self, consumer, partitions):
        self.first_read.update((item.topic, item.partition) for item in partitions)
        logging.info("assigned partitions=%s", [item.partition for item in partitions])

    def revoke(self, consumer, partitions):
        keys = {(item.topic, item.partition) for item in partitions}
        self.commit(consumer, keys)
        self.forget(keys)
        logging.info("revoked partitions=%s", [item.partition for item in partitions])

    def lost(self, consumer, partitions):
        self.forget({(item.topic, item.partition) for item in partitions})
        logging.warning("lost partitions=%s", [item.partition for item in partitions])

    def forget(self, keys):
        self.first_read.difference_update(keys)
        for key in keys:
            self.pending.pop(key, None)

    def commit(self, consumer, keys=None):
        offsets = [
            TopicPartition(topic, partition, offset)
            for (topic, partition), offset in self.pending.items()
            if keys is None or (topic, partition) in keys
        ]
        if not offsets:
            return
        try:
            results = consumer.commit(offsets=offsets, asynchronous=False) or []
        except KafkaException as error:
            logging.warning("commit failed: %s", error)
            return

        committed = {}
        for item in results:
            if item.error:
                logging.warning("commit failed partition=%s: %s", item.partition, item.error)
                continue
            self.pending.pop((item.topic, item.partition), None)
            committed[item.partition] = item.offset
        if committed:
            logging.info("committed next offsets=%s", committed)


def dead_letter(producer, message, reason):
    error, _ = send(
        producer,
        DEAD_LETTER_TOPIC,
        message.value(),
        key=message.key(),
        partition=message.partition(),
        headers=[
            ("error-reason", reason),
            ("source-topic", message.topic()),
            ("source-partition", str(message.partition())),
            ("source-offset", str(message.offset())),
            ("attempts", "3"),
        ],
    )
    if error is not None:
        raise KafkaException(error)


def process(connection, producer, message, consumer_id, delay, stopping, errors):
    event = json.loads(message.value())
    for attempt in range(1, 4):
        if stopping.wait(delay):
            return None
        try:
            code = check_device(event)
        except UnknownDevice as error:
            errors["unknown_device"].inc()
            logging.warning("processing failed attempt=%s event=%s: %s", attempt, event["event_id"], error)
            if attempt < 3:
                continue
            dead_letter(producer, message, str(error))
            return "dlt"

        if save_event(connection, message, event, code, consumer_id):
            return "saved"
        logging.info("duplicate event=%s partition=%s offset=%s",
                     event["event_id"], message.partition(), message.offset())
        return "duplicate"
    return None


def run():
    configure_logging()
    stopping = stop_event()
    consumer_id = int(os.environ["CONSUMER_ID"])
    delay = int(os.environ.get("PROCESSING_DELAY_MS", "45")) / 1000
    interval = int(os.environ.get("COMMIT_INTERVAL_MS", "2000")) / 1000
    client_id = f"consumer-{consumer_id}"
    outcomes = {outcome: RESULTS.labels(client_id, outcome) for outcome in ("saved", "duplicate", "dlt")}
    errors = {reason: ERRORS.labels(client_id, reason) for reason in ("unknown_device", "dlt_publish")}

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP,
            "client.id": client_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 10000,
            "heartbeat.interval.ms": 3000,
        }
    )
    producer = kafka_producer(f"{client_id}-dlt")
    offsets = ProcessedOffsets()
    consumer.subscribe([TOPIC], on_assign=offsets.assign, on_revoke=offsets.revoke, on_lost=offsets.lost)
    start_http_server(9100)

    try:
        with database() as connection:
            commit_at = monotonic() + interval
            while not stopping.is_set():
                if monotonic() >= commit_at:
                    offsets.commit(consumer)
                    commit_at = monotonic() + interval

                message = consumer.poll(0.2)
                if message is None:
                    continue
                if message.error():
                    logging.warning("poll: %s", message.error())
                    continue

                key = message.topic(), message.partition()
                if key in offsets.first_read:
                    logging.info("first read after assignment partition=%s offset=%s", key[1], message.offset())
                    offsets.first_read.discard(key)
                try:
                    outcome = process(connection, producer, message, consumer_id, delay, stopping, errors)
                except KafkaException as error:
                    errors["dlt_publish"].inc()
                    logging.warning("DLT unavailable partition=%s offset=%s: %s", key[1], message.offset(), error)
                    consumer.seek(TopicPartition(*key, message.offset()))
                    stopping.wait(0.5)
                    continue
                if outcome is not None:
                    outcomes[outcome].inc()
                    offsets.pending[key] = message.offset() + 1
    finally:
        offsets.commit(consumer)
        consumer.close()
        producer.flush(10)


if __name__ == "__main__":
    run()
