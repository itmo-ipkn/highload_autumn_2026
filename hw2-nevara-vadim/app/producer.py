import json
import logging
import os
import random
import signal
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import cycle
from time import monotonic, sleep
from uuid import uuid4

import psycopg
from confluent_kafka import KafkaError, Message, Producer
from psycopg.types.json import Jsonb

LOGGER = logging.getLogger(__name__)
TOPIC = "events"
EVENT_TYPES = ("page_view", "add_to_cart", "purchase")
PRODUCER_IDS = {"p1", "p2", "p3"}
POLL_INTERVAL_SECONDS = 0.2


@dataclass(frozen=True)
class Settings:
    producer_id: str
    user_count: int
    interval_seconds: float
    poison_rate: float
    key_mode: str

    @classmethod
    def from_env(cls) -> "Settings":
        producer_id = os.environ["PRODUCER_ID"]
        if producer_id not in PRODUCER_IDS:
            raise ValueError("PRODUCER_ID must be p1, p2 or p3")

        key_mode = os.getenv("KEY_MODE", "user_id")
        if key_mode not in {"user_id", "none"}:
            raise ValueError("KEY_MODE must be user_id or none")

        user_count = int(os.getenv("USERS_PER_PRODUCER", "2"))
        interval_ms = int(os.getenv("PRODUCE_INTERVAL_MS", "150"))
        poison_rate = float(os.getenv("POISON_RATE", "0.02"))
        if user_count < 1 or interval_ms < 0 or not 0 <= poison_rate <= 1:
            raise ValueError(
                "Expected USERS_PER_PRODUCER >= 1, PRODUCE_INTERVAL_MS >= 0 "
                "and POISON_RATE in [0, 1]"
            )
        return cls(producer_id, user_count, interval_ms / 1000, poison_rate, key_mode)

    def user_ids(self) -> list[str]:
        return [
            f"{self.producer_id}-u{index}" for index in range(1, self.user_count + 1)
        ]


def make_event(user_id: str, seq: int, poison_rate: float) -> dict:
    return {
        "event_id": str(uuid4()),
        "user_id": user_id,
        "type": random.choice(EVENT_TYPES),
        "seq": seq,
        "produced_at": datetime.now(timezone.utc).isoformat(),
        "payload": {"poison": random.random() < poison_rate},
    }


def prepare_event(
    connection: psycopg.Connection, settings: Settings, user_id: str
) -> dict:
    with connection.transaction():
        connection.execute(
            """
            INSERT INTO producer_state (user_id, owner_id)
            VALUES (%s, %s)
            ON CONFLICT (user_id) DO NOTHING
            """,
            (user_id, settings.producer_id),
        )
        state = connection.execute(
            """
            SELECT last_acked_seq, pending_event
            FROM producer_state
            WHERE user_id = %s AND owner_id = %s
            FOR UPDATE
            """,
            (user_id, settings.producer_id),
        ).fetchone()
        if state is None:
            raise RuntimeError(f"User {user_id} belongs to another producer")

        last_acked_seq, pending_event = state
        if pending_event is not None:
            return pending_event

        event = make_event(user_id, last_acked_seq + 1, settings.poison_rate)
        connection.execute(
            "UPDATE producer_state SET pending_event = %s WHERE user_id = %s",
            (Jsonb(event), user_id),
        )
        return event


def acknowledge(connection: psycopg.Connection, producer_id: str, event: dict) -> None:
    updated = connection.execute(
        """
        UPDATE producer_state
        SET last_acked_seq = %s, pending_event = NULL
        WHERE user_id = %s AND owner_id = %s
          AND last_acked_seq = %s AND pending_event->>'event_id' = %s
        """,
        (
            event["seq"],
            event["user_id"],
            producer_id,
            event["seq"] - 1,
            event["event_id"],
        ),
    )
    if updated.rowcount != 1:
        raise RuntimeError(f"Pending event changed for {event['user_id']}")


@dataclass
class Shutdown:
    requested: bool = False

    def request(self, _signum, _frame) -> None:
        self.requested = True

    def wait(self, seconds: float) -> None:
        deadline = monotonic() + seconds
        while not self.requested:
            remaining = deadline - monotonic()
            if remaining <= 0:
                return
            sleep(min(POLL_INTERVAL_SECONDS, remaining))


@dataclass
class Delivery:
    event: dict
    acknowledged: bool | None = None

    def on_delivery(self, error: KafkaError | None, message: Message) -> None:
        self.acknowledged = error is None
        if error is not None:
            LOGGER.warning(
                "delivery failed user_id=%s seq=%s event_id=%s "
                "(pending event retained): %s",
                self.event["user_id"],
                self.event["seq"],
                self.event["event_id"],
                error,
            )
            return

        LOGGER.debug(
            "delivered user_id=%s seq=%s partition=%s offset=%s",
            self.event["user_id"],
            self.event["seq"],
            message.partition(),
            message.offset(),
        )


def enqueue(producer: Producer, delivery: Delivery, key_mode: str) -> None:
    key = delivery.event["user_id"].encode("utf-8") if key_mode == "user_id" else None
    producer.produce(
        TOPIC,
        key=key,
        value=json.dumps(delivery.event).encode("utf-8"),
        on_delivery=delivery.on_delivery,
    )


def run_producer(
    producer: Producer,
    connection: psycopg.Connection,
    settings: Settings,
    shutdown: Shutdown,
) -> None:
    in_flight = None
    event = None
    user_ids = cycle(settings.user_ids())
    try:
        pending = connection.execute(
            """
            SELECT pending_event
            FROM producer_state
            WHERE owner_id = %s AND pending_event IS NOT NULL
            ORDER BY user_id
            """,
            (settings.producer_id,),
        )
        recovered = (row[0] for row in pending)
        while not shutdown.requested:
            if event is None:
                event = next(recovered, None)
                if event is None:
                    event = prepare_event(connection, settings, next(user_ids))
            in_flight = Delivery(event)
            if shutdown.requested:
                break

            enqueue(producer, in_flight, settings.key_mode)
            while in_flight.acknowledged is None and not shutdown.requested:
                producer.poll(POLL_INTERVAL_SECONDS)
            if in_flight.acknowledged is None:
                break

            acknowledged = in_flight.acknowledged
            in_flight = None
            if acknowledged:
                acknowledge(connection, settings.producer_id, event)
                event = None
            shutdown.wait(settings.interval_seconds)
    finally:
        unflushed = producer.flush(6)
        if in_flight is not None and in_flight.acknowledged:
            acknowledge(connection, settings.producer_id, in_flight.event)
        elif in_flight is not None and in_flight.acknowledged is None:
            LOGGER.warning(
                "exiting with pending event user_id=%s seq=%s event_id=%s unflushed=%s",
                in_flight.event["user_id"],
                in_flight.event["seq"],
                in_flight.event["event_id"],
                unflushed,
            )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    settings = Settings.from_env()
    shutdown = Shutdown()
    signal.signal(signal.SIGTERM, shutdown.request)
    signal.signal(signal.SIGINT, shutdown.request)

    with psycopg.connect(
        os.environ["DATABASE_URL"],
        autocommit=True,
        connect_timeout=5,
        options="-c statement_timeout=5000",
    ) as connection:
        locked, = connection.execute(
            "SELECT pg_try_advisory_lock(hashtext('ddia-hw2-producer'), hashtext(%s))",
            (settings.producer_id,),
        ).fetchone()
        if not locked:
            raise RuntimeError(f"Producer {settings.producer_id} is already running")

        producer = Producer(
            {
                "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
                "client.id": settings.producer_id,
                "acks": "all",
                "enable.idempotence": True,
                "partitioner": "murmur2_random",
                "sticky.partitioning.linger.ms": 0,
                "message.timeout.ms": 5000,
            },
            logger=LOGGER,
        )
        LOGGER.info(
            "started producer_id=%s users=%s key_mode=%s",
            settings.producer_id,
            settings.user_count,
            settings.key_mode,
        )
        run_producer(producer, connection, settings, shutdown)


if __name__ == "__main__":
    main()
