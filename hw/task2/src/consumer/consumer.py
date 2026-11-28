#!/usr/bin/env python3

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import psycopg2
from kafka import KafkaConsumer, KafkaProducer
from kafka.consumer.subscription_state import ConsumerRebalanceListener
from kafka.structs import OffsetAndMetadata, TopicPartition


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] [consumer-%(consumer_id)s] %(message)s",
    stream=sys.stdout,
)


class _DefaultConsumerId(logging.Filter):

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "consumer_id"):
            record.consumer_id = "shared"
        return True


for _handler in logging.getLogger().handlers:
    _handler.addFilter(_DefaultConsumerId())



logging.getLogger("kafka").setLevel(logging.WARNING)


class ConsumerLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg: str, kwargs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        extra = kwargs.setdefault("extra", {})
        extra.setdefault("consumer_id", self.extra["consumer_id"])
        return msg, kwargs


CONSUMER_ID = int(os.getenv("CONSUMER_ID", "1"))
BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092",
).split(",")
EVENTS_TOPIC = os.getenv("EVENTS_TOPIC", "events")
EVENTS_DLT_TOPIC = os.getenv("EVENTS_DLT_TOPIC", "events.dlt")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "events-processor")
PROCESSING_DELAY_MS = int(os.getenv("PROCESSING_DELAY_MS", "100"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_BACKOFF_MS = int(os.getenv("RETRY_BACKOFF_MS", "500"))
DLT_RETRY_DELAY_SECONDS = float(os.getenv("DLT_RETRY_DELAY_SECONDS", "2"))
DB_RECONNECT_DELAY_SECONDS = float(os.getenv("DB_RECONNECT_DELAY_SECONDS", "2"))
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "earliest").strip().lower()
MAX_POLL_RECORDS = int(os.getenv("MAX_POLL_RECORDS", "10"))

if CONSUMER_ID < 1:
    raise ValueError("CONSUMER_ID must be positive")
if PROCESSING_DELAY_MS < 0:
    raise ValueError("PROCESSING_DELAY_MS cannot be negative")
if MAX_RETRIES < 1:
    raise ValueError("MAX_RETRIES must be at least 1")
if AUTO_OFFSET_RESET not in {"earliest", "latest"}:
    raise ValueError("AUTO_OFFSET_RESET must be 'earliest' or 'latest'")

logger = ConsumerLoggerAdapter(logging.getLogger(__name__), {"consumer_id": CONSUMER_ID})
shutdown_event = threading.Event()

DB_CONFIG = {
    "host": os.getenv("SINK_DB_HOST", "kafka-sink"),
    "port": int(os.getenv("SINK_DB_PORT", "5432")),
    "dbname": os.getenv("SINK_DB_NAME", "sink_db"),
    "user": os.getenv("SINK_DB_USER", "kafka"),
    "password": os.getenv("SINK_DB_PASSWORD", "kafka"),
    "connect_timeout": 5,
}


class ProcessingError(Exception):
    pass


class PoisonEventError(ProcessingError):
    pass


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def parse_event(raw_value: bytes) -> dict[str, Any]:
    try:
        event = json.loads(raw_value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProcessingError(f"Invalid JSON: {exc}") from exc

    if not isinstance(event, dict):
        raise ProcessingError("Event must be a JSON object")

    required = ("event_id", "user_id", "type", "seq", "produced_at", "payload")
    missing = [field for field in required if field not in event]
    if missing:
        raise ProcessingError(f"Missing required field(s): {', '.join(missing)}")

    try:
        event["event_id"] = str(uuid.UUID(str(event["event_id"])))
    except (ValueError, TypeError, AttributeError) as exc:
        raise ProcessingError(f"event_id is not a UUID: {event['event_id']!r}") from exc

    if not isinstance(event["user_id"], str) or not event["user_id"]:
        raise ProcessingError("user_id must be a non-empty string")
    if isinstance(event["seq"], bool) or not isinstance(event["seq"], int) or event["seq"] < 1:
        raise ProcessingError("seq must be a positive integer")
    if event["type"] not in {"page_view", "add_to_cart", "purchase"}:
        raise ProcessingError(f"Unsupported event type: {event['type']!r}")
    if not isinstance(event["payload"], dict):
        raise ProcessingError("payload must be a JSON object")

    return event


def process_event(event: dict[str, Any]) -> None:
    if PROCESSING_DELAY_MS:
        time.sleep(PROCESSING_DELAY_MS / 1000.0)

    if event["payload"].get("poison") is True:
        raise PoisonEventError("Poison event: payload.poison=true")


def connect_db() -> Any:
    connection = psycopg2.connect(**DB_CONFIG)
    connection.autocommit = False
    return connection


def write_to_sink(connection: Any, event: dict[str, Any], record: Any) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO processed_events (
                event_id, user_id, seq, consumer_id, topic,
                "partition", kafka_offset
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            (
                event["event_id"],
                event["user_id"],
                event["seq"],
                CONSUMER_ID,
                record.topic,
                record.partition,
                record.offset,
            ),
        )
        inserted = cursor.rowcount == 1

        if not inserted:
            cursor.execute(
                """
                INSERT INTO consumer_stats (consumer_id, duplicate_count)
                VALUES (%s, 1)
                ON CONFLICT (consumer_id) DO UPDATE
                SET duplicate_count = consumer_stats.duplicate_count + 1,
                    updated_at = clock_timestamp()
                """,
                (CONSUMER_ID,),
            )

    connection.commit()
    return inserted


class RebalanceListener(ConsumerRebalanceListener):

    def __init__(self, app: "ConsumerApp") -> None:
        self.app = app

    def on_partitions_revoked(self, revoked: list[TopicPartition]) -> None:
        logger.info("Partitions revoked: %s", revoked)
        offsets = {
            tp: OffsetAndMetadata(self.app.safe_offsets[tp], "")
            for tp in revoked
            if tp in self.app.safe_offsets
        }
        if offsets:
            if self.app.commit_offsets(offsets, reason="before rebalance revoke"):
                logger.info("Committed offsets before revoke: %s", offsets)
            else:
                logger.error("Could not commit offsets before revoke: %s", offsets)
        for tp in revoked:
            self.app.safe_offsets.pop(tp, None)

    def on_partitions_assigned(self, assigned: list[TopicPartition]) -> None:
        logger.info("Partitions assigned: %s", assigned)
        for tp in assigned:

            self.app.safe_offsets.pop(tp, None)


class ConsumerApp:
    def __init__(self) -> None:
        self.db: Any = None
        self.consumer: KafkaConsumer | None = None
        self.dlt_producer: KafkaProducer | None = None
        self.safe_offsets: dict[TopicPartition, int] = {}
        self.stats = {
            "processed": 0,
            "duplicates": 0,
            "dlt": 0,
            "processing_errors": 0,
            "db_reconnects": 0,
            "commit_failures": 0,
        }


    def ensure_db(self) -> bool:
        if self.db is not None and not self.db.closed:
            return True
        while not shutdown_event.is_set():
            try:
                self.db = connect_db()
                logger.info("Connected to PostgreSQL sink")
                return True
            except psycopg2.Error as exc:
                self.stats["db_reconnects"] += 1
                logger.error("Cannot connect to PostgreSQL sink: %s; retrying", exc)
                time.sleep(DB_RECONNECT_DELAY_SECONDS)
        return False

    def rollback_db(self) -> None:
        if self.db is not None:
            try:
                self.db.rollback()
            except Exception:
                pass

    def drop_db(self) -> None:
        if self.db is not None:
            try:
                self.db.rollback()
            except Exception:
                pass
            try:
                self.db.close()
            except Exception:
                pass
        self.db = None


    def commit_offsets(
        self, offsets: dict[TopicPartition, OffsetAndMetadata], reason: str
    ) -> bool:
        assert self.consumer is not None
        for attempt in range(1, 4):
            try:
                self.consumer.commit(offsets=offsets)
                return True
            except Exception as exc:
                self.stats["commit_failures"] += 1
                logger.warning(
                    "Offset commit failed (%s), attempt %s/3: %s", reason, attempt, exc
                )
                if shutdown_event.is_set():
                    break
                time.sleep(0.5 * attempt)
        return False


    def publish_to_dlt(
        self,
        record: Any,
        error: Exception,
        attempts: int,
    ) -> bool:
        assert self.dlt_producer is not None
        headers = [
            ("error_reason", str(error)[:2000].encode("utf-8", errors="replace")),
            ("error_class", type(error).__name__.encode("utf-8", errors="replace")),
            ("original_topic", record.topic.encode("utf-8")),
            ("original_partition", str(record.partition).encode("ascii")),
            ("original_offset", str(record.offset).encode("ascii")),
            ("attempts", str(attempts).encode("ascii")),
            ("consumer_id", str(CONSUMER_ID).encode("ascii")),
            ("failed_at", utc_now_iso().encode("ascii")),
        ]

        while not shutdown_event.is_set():
            try:
                future = self.dlt_producer.send(
                    EVENTS_DLT_TOPIC,
                    key=record.key,
                    value=record.value,
                    headers=headers,
                )

                future.get(timeout=30)
                self.stats["dlt"] += 1
                logger.warning(
                    "Published to DLT: source=%s[%s]@%s attempts=%s reason=%s",
                    record.topic,
                    record.partition,
                    record.offset,
                    attempts,
                    error,
                )
                return True
            except Exception as exc:
                logger.error(
                    "Could not publish to DLT source=%s[%s]@%s: %s; retrying",
                    record.topic,
                    record.partition,
                    record.offset,
                    exc,
                )
                time.sleep(DLT_RETRY_DELAY_SECONDS)

        logger.error(
            "Stopping before DLT publication of %s[%s]@%s; source offset stays uncommitted",
            record.topic,
            record.partition,
            record.offset,
        )
        return False


    def handle_record(self, record: Any) -> None:
        assert self.consumer is not None
        tp = TopicPartition(record.topic, record.partition)
        processing_attempts = 0

        while not shutdown_event.is_set():
            try:
                event = parse_event(record.value)
                process_event(event)
                inserted = write_to_sink(self.db, event, record)

                if inserted:
                    self.stats["processed"] += 1
                    logger.info(
                        "Processed event_id=%s user=%s seq=%s source=%s[%s]@%s",
                        event["event_id"],
                        event["user_id"],
                        event["seq"],
                        record.topic,
                        record.partition,
                        record.offset,
                    )
                else:
                    self.stats["duplicates"] += 1
                    logger.info(
                        "Duplicate suppressed event_id=%s user=%s seq=%s source=%s[%s]@%s",
                        event["event_id"],
                        event["user_id"],
                        event["seq"],
                        record.topic,
                        record.partition,
                        record.offset,
                    )

                self.safe_offsets[tp] = record.offset + 1
                self.commit_offsets(
                    {tp: OffsetAndMetadata(record.offset + 1, "")},
                    reason="after sink write",
                )
                return

            except ProcessingError as exc:
                self.rollback_db()
                processing_attempts += 1
                self.stats["processing_errors"] += 1
                logger.warning(
                    "Processing failed source=%s[%s]@%s attempt=%s/%s: %s",
                    record.topic,
                    record.partition,
                    record.offset,
                    processing_attempts,
                    MAX_RETRIES,
                    exc,
                )
                if processing_attempts >= MAX_RETRIES:
                    if self.publish_to_dlt(record, exc, processing_attempts):
                        self.safe_offsets[tp] = record.offset + 1
                        self.commit_offsets(
                            {tp: OffsetAndMetadata(record.offset + 1, "")},
                            reason="after DLT publish",
                        )
                    return
                if shutdown_event.is_set():
                    return
                time.sleep((RETRY_BACKOFF_MS * processing_attempts) / 1000.0)

            except psycopg2.Error as exc:



                logger.error(
                    "Sink write failed source=%s[%s]@%s: %s; reconnecting",
                    record.topic,
                    record.partition,
                    record.offset,
                    exc,
                )
                self.drop_db()
                if not self.ensure_db():
                    return
                time.sleep(DB_RECONNECT_DELAY_SECONDS)


    def run(self) -> None:
        self.ensure_db()

        self.consumer = KafkaConsumer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP,
            client_id=f"events-processor-{CONSUMER_ID}",
            enable_auto_commit=False,



            auto_offset_reset=AUTO_OFFSET_RESET,
            max_poll_records=MAX_POLL_RECORDS,
            max_poll_interval_ms=300_000,
            session_timeout_ms=30_000,
            heartbeat_interval_ms=3_000,
            request_timeout_ms=40_000,
            value_deserializer=None,
            key_deserializer=None,
        )

        self.dlt_producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            client_id=f"events-processor-{CONSUMER_ID}-dlt",
            acks="all",
            retries=10,
            max_in_flight_requests_per_connection=1,
            linger_ms=0,
            request_timeout_ms=30_000,
            max_block_ms=60_000,
        )

        listener = RebalanceListener(self)
        self.consumer.subscribe([EVENTS_TOPIC], listener=listener)
        logger.info(
            "Started group=%s brokers=%s auto_offset_reset=%s delay_ms=%s max_retries=%s",
            CONSUMER_GROUP,
            BOOTSTRAP_SERVERS,
            AUTO_OFFSET_RESET,
            PROCESSING_DELAY_MS,
            MAX_RETRIES,
        )

        while not shutdown_event.is_set():
            records = self.consumer.poll(timeout_ms=1000)
            for tp, partition_records in records.items():
                for record in partition_records:
                    if shutdown_event.is_set():
                        break
                    self.handle_record(record)
                if shutdown_event.is_set():
                    break


    def close(self) -> None:
        if self.consumer is not None and self.safe_offsets:
            offsets = {
                tp: OffsetAndMetadata(offset, "")
                for tp, offset in self.safe_offsets.items()
            }
            self.commit_offsets(offsets, reason="before consumer shutdown")

        if self.dlt_producer is not None:
            try:
                remaining = self.dlt_producer.flush(timeout=30)
                if remaining:
                    logger.error("%s DLT message(s) remain queued at shutdown", remaining)
            finally:
                self.dlt_producer.close(timeout=30)

        if self.consumer is not None:
            try:
                self.consumer.close(autocommit=False)
            except Exception:
                logger.exception("Error while closing Kafka consumer")

        self.drop_db()


app: ConsumerApp | None = None


def signal_handler(signum: int, _frame: Any) -> None:
    logger.info("Received signal %s; finishing current record", signum)
    shutdown_event.set()


def main() -> None:
    global app

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    app = ConsumerApp()
    try:
        app.run()
    except Exception:
        logger.exception("Consumer terminated due to an unexpected error")
        raise
    finally:
        app.close()
        logger.info(
            "Stopped. processed=%s duplicates=%s dlt=%s processing_errors=%s "
            "db_reconnects=%s commit_failures=%s",
            app.stats["processed"],
            app.stats["duplicates"],
            app.stats["dlt"],
            app.stats["processing_errors"],
            app.stats["db_reconnects"],
            app.stats["commit_failures"],
        )


if __name__ == "__main__":
    main()
