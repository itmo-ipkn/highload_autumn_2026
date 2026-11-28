import json
import logging
import os
import signal
import time
from collections import Counter
from datetime import datetime, timedelta
from uuid import UUID

import psycopg
from confluent_kafka import Consumer, KafkaException, Message, Producer, TopicPartition

from .sink import MAX_ATTEMPTS, Sink


SOURCE_TOPIC = "events"
DLT_TOPIC = "events.dlt"
GROUP_ID = "events-processor"
EVENT_TYPES = {"page_view", "add_to_cart", "purchase"}
COMMIT_BATCH_SIZE = 10
COMMIT_INTERVAL = 1.0

Partition = tuple[str, int]
Source = tuple[str, int, int]

LOG = logging.getLogger(__name__)


class InvalidEvent(ValueError):
    pass


class PoisonEvent(ValueError):
    pass


class DltUnavailable(RuntimeError):
    pass


def decode_event(value: bytes | None) -> dict:
    if value is None:
        raise InvalidEvent("event value is absent")
    try:
        event = json.loads(value)
    except ValueError as error:
        raise InvalidEvent("event value is not valid JSON") from error
    if not isinstance(event, dict):
        raise InvalidEvent("event value must be a JSON object")
    return event


def event_identity(event: dict) -> tuple[UUID, str, int]:
    event_id = event.get("event_id")
    if not isinstance(event_id, str):
        raise InvalidEvent("event_id must be a UUID")
    try:
        event_id = UUID(event_id)
    except ValueError as error:
        raise InvalidEvent("event_id must be a UUID") from error
    user_id = event.get("user_id")
    if not isinstance(user_id, str) or not user_id or "\x00" in user_id:
        raise InvalidEvent("user_id must be a nonempty string")
    try:
        user_id.encode("utf-8")
    except UnicodeEncodeError as error:
        raise InvalidEvent("user_id must be valid UTF-8") from error
    seq = event.get("seq")
    if type(seq) is not int or not 1 <= seq < 2**63:
        raise InvalidEvent("seq must be a positive 64-bit integer")
    return event_id, user_id, seq


def validate_event(event: dict) -> tuple[UUID, str, int]:
    identity = event_identity(event)
    event_type = event.get("type")
    if not isinstance(event_type, str) or event_type not in EVENT_TYPES:
        raise InvalidEvent("unknown event type")
    produced_at = event.get("produced_at")
    if not isinstance(produced_at, str):
        raise InvalidEvent("produced_at must be an ISO-8601 timestamp in UTC")
    try:
        timestamp = datetime.fromisoformat(produced_at)
    except ValueError as error:
        raise InvalidEvent("produced_at must be an ISO-8601 timestamp in UTC") from error
    if timestamp.utcoffset() != timedelta(0):
        raise InvalidEvent("produced_at must be an ISO-8601 timestamp in UTC")
    payload = event.get("payload")
    if not isinstance(payload, dict) or not isinstance(payload.get("poison"), bool):
        raise InvalidEvent("payload.poison must be boolean")
    if payload["poison"]:
        raise PoisonEvent("payload.poison is true")
    return identity


class ReadyOffsets:
    def __init__(self) -> None:
        self.offsets: dict[Partition, int] = {}
        self.batch: Counter[Partition] = Counter()
        self.flush_at: float | None = None
        self.awaiting_first_record: set[Partition] = set()

    def log_first_record(self, source: Source) -> None:
        topic, partition, offset = source
        key = (topic, partition)
        if key in self.awaiting_first_record:
            self.awaiting_first_record.remove(key)
            LOG.info("first consumed topic=%s partition=%s offset=%s", topic, partition, offset)

    def mark(self, source: Source) -> None:
        topic, partition, offset = source
        self.offsets[topic, partition] = offset + 1
        self.batch[topic, partition] += 1
        if self.flush_at is None:
            self.flush_at = time.monotonic() + COMMIT_INTERVAL

    def poll_timeout(self) -> float:
        if self.flush_at is None:
            return COMMIT_INTERVAL
        return max(0.0, self.flush_at - time.monotonic())

    def flush_if_due(self, consumer: Consumer) -> None:
        if not self.offsets:
            return
        if self.batch.total() < COMMIT_BATCH_SIZE and self.poll_timeout() > 0:
            return
        self.commit(consumer)
        self.batch.clear()
        self.flush_at = time.monotonic() + COMMIT_INTERVAL if self.offsets else None

    def commit(
        self, consumer: Consumer, partitions: list[TopicPartition] | None = None
    ) -> None:
        keys = self.offsets.keys()
        if partitions is not None:
            keys = {(partition.topic, partition.partition) for partition in partitions}
        offsets = [TopicPartition(*key, self.offsets[key]) for key in keys if key in self.offsets]
        if not offsets:
            return
        try:
            committed = consumer.commit(offsets=offsets, asynchronous=False)
        except KafkaException as error:
            LOG.warning("offset commit failed offsets=%s error=%s", offsets, error)
            return
        for partition in committed:
            if partition.error is not None:
                LOG.warning("offset commit failed partition=%s", partition)
                continue
            self.offsets.pop((partition.topic, partition.partition), None)

    def discard(self, partitions: list[TopicPartition]) -> None:
        for partition in partitions:
            key = (partition.topic, partition.partition)
            self.offsets.pop(key, None)
            self.batch.pop(key, None)
            self.awaiting_first_record.discard(key)
        if not self.offsets:
            self.flush_at = None

    def on_assign(self, _consumer: Consumer, partitions: list[TopicPartition]) -> None:
        self.awaiting_first_record.update((part.topic, part.partition) for part in partitions)
        LOG.info("assigned partitions=%s", partitions)

    def on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        LOG.info("revoked partitions=%s", partitions)
        self.commit(consumer, partitions)
        self.discard(partitions)

    def on_lost(self, _consumer: Consumer, partitions: list[TopicPartition]) -> None:
        LOG.warning("lost partitions=%s", partitions)
        self.discard(partitions)


def publish_dlt(
    producer: Producer,
    message: Message,
    event: dict,
    source: Source,
    attempts: int,
    error: InvalidEvent | PoisonEvent,
) -> None:
    topic, partition, offset = source
    headers = {
        "error-type": type(error).__name__,
        "error-message": str(error),
        "processing-attempts": attempts,
        "source-topic": topic,
        "source-partition": partition,
        "source-offset": offset,
        "event-id": event.get("event_id", ""),
        "user-id": event.get("user_id", ""),
        "seq": event.get("seq", ""),
    }
    delivery = []

    def on_delivery(delivery_error, _delivered_message):
        delivery.append(delivery_error)

    producer.produce(
        DLT_TOPIC,
        value=message.value(),
        key=message.key(),
        headers={
            name: str(value).encode("utf-8", errors="replace")
            for name, value in headers.items()
        },
        on_delivery=on_delivery,
    )
    producer.flush(7)
    if not delivery:
        raise DltUnavailable("DLT delivery acknowledgement timed out")
    if delivery[0] is not None:
        raise DltUnavailable(f"DLT delivery failed: {delivery[0]}")


def process_message(
    message: Message,
    sink: Sink,
    producer: Producer,
    consumer_id: str,
) -> bool:
    source = (message.topic(), message.partition(), message.offset())
    event = {}
    try:
        event = decode_event(message.value())
        event_id, user_id, seq = validate_event(event)
    except (InvalidEvent, PoisonEvent) as error:
        attempts = sink.record_failure(source, str(error))
        LOG.warning("processing failure source=%s attempts=%s error=%s", source, attempts, error)
        if attempts < MAX_ATTEMPTS:
            return False
        publish_dlt(producer, message, event, source, attempts, error)
        LOG.info("DLT acknowledged source=%s attempts=%s", source, attempts)
        return True
    inserted = sink.store_event(event_id, user_id, seq, consumer_id, source)
    if not inserted:
        LOG.info("duplicate event_id=%s source=%s", event_id, source)
    return True


def run_consumer(
    consumer: Consumer, producer: Producer, sink: Sink, consumer_id: str, delay: float
) -> None:
    stopping = False

    def stop(_signum, _frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    ready = ReadyOffsets()
    consumer.subscribe(
        [SOURCE_TOPIC],
        on_assign=ready.on_assign,
        on_revoke=ready.on_revoke,
        on_lost=ready.on_lost,
    )
    try:
        while not stopping:
            ready.flush_if_due(consumer)
            message = consumer.poll(ready.poll_timeout())
            if message is None:
                continue
            error = message.error()
            if error is not None:
                if error.fatal():
                    raise KafkaException(error)
                LOG.warning("source poll failed error=%s", error)
                continue
            if stopping:
                break
            source = (message.topic(), message.partition(), message.offset())
            ready.log_first_record(source)
            time.sleep(delay)
            try:
                processed = process_message(message, sink, producer, consumer_id)
            except (
                psycopg.OperationalError,
                psycopg.InterfaceError,
                KafkaException,
                BufferError,
                DltUnavailable,
            ) as error:
                LOG.warning("infrastructure failure source=%s error=%s", source, error)
                processed = False
                time.sleep(0.5)
            if processed:
                ready.mark(source)
            elif not stopping:
                # Retry before accepting a later offset from this partition.
                consumer.seek(TopicPartition(*source))
    finally:
        try:
            try:
                if ready.offsets:
                    ready.commit(consumer, consumer.assignment())
            finally:
                consumer.close()
        finally:
            sink.close()


def main() -> None:
    consumer_id = os.environ["CONSUMER_ID"]
    logging.basicConfig(
        format=f"%(asctime)s %(levelname)s consumer={consumer_id} %(message)s",
        level=logging.INFO,
    )
    delay = float(os.environ.get("PROCESSING_DELAY_MS", "60")) / 1000
    if delay < 0:
        raise ValueError("PROCESSING_DELAY_MS cannot be negative")
    bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "client.id": consumer_id,
        "group.id": GROUP_ID,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
        "session.timeout.ms": 10000,
    })
    producer = Producer({
        "bootstrap.servers": bootstrap_servers,
        "client.id": f"{consumer_id}-dlt",
        "acks": "all",
        "enable.idempotence": True,
        "message.timeout.ms": 5000,
    })
    sink = Sink(os.environ["DATABASE_URL"])
    run_consumer(consumer, producer, sink, consumer_id, delay)


if __name__ == "__main__":
    main()
