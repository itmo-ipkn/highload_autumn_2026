import json
import logging
import os
from time import monotonic

from prometheus_client import Counter, start_http_server
from psycopg.types.json import Jsonb

from .events import ACTIONS, ADMINS_PER_PRODUCER, new_event
from .settings import PARTITIONS, TOPIC, configure_logging, database, kafka_producer, send, stop_event

PUBLISHED = Counter("netinventory_events_published", "Acknowledged Kafka publications", ("producer", "mode"))
ERRORS = Counter("netinventory_publish_errors", "Failed Kafka delivery reports", ("producer", "mode"))


def register_admin(connection, user_id, mode):
    connection.execute(
        "INSERT INTO producer_progress (user_id, mode) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        (user_id, mode),
    )


def next_event(connection, user_id, number, mode):
    seq, pending = connection.execute(
        "SELECT confirmed_seq, pending FROM producer_progress WHERE user_id = %s", (user_id,)
    ).fetchone()
    if pending is not None:
        return pending

    event = new_event(user_id, number, seq + 1, mode)
    connection.execute(
        "UPDATE producer_progress SET pending = %s WHERE user_id = %s",
        (Jsonb(event), user_id),
    )
    return event


def run():
    configure_logging()
    stopping = stop_event()
    producer_id = int(os.environ["PRODUCER_ID"])
    mode = os.environ.get("KEY_MODE", "user_id")
    if mode not in {"user_id", "none"}:
        raise SystemExit("KEY_MODE must be user_id or none")

    rate = float(os.environ.get("EVENTS_PER_SECOND", "12"))
    if rate <= 0:
        raise SystemExit("EVENTS_PER_SECOND must be positive")

    client_id = f"producer-{producer_id}"
    published = PUBLISHED.labels(client_id, mode)
    errors = ERRORS.labels(client_id, mode)
    start_http_server(9100)
    producer = kafka_producer(client_id)

    start = (producer_id - 1) * ADMINS_PER_PRODUCER + 1
    prefix = "admin" if mode == "user_id" else "admin-unkeyed"
    users = [(f"{prefix}-{number:02d}", number) for number in range(start, start + ADMINS_PER_PRODUCER)]
    logging.info("started mode=%s users=%s", mode, ",".join(user for user, _ in users))

    try:
        with database() as connection:
            for user_id, _ in users:
                register_admin(connection, user_id, mode)

            index = 0
            request_started = monotonic()
            recovering = False
            while not stopping.is_set():
                user_id, number = users[index]
                event = next_event(connection, user_id, number, mode)
                seq = event["seq"]
                routing = {"key": user_id}
                if mode == "none":
                    # Alternate partitions for each admin so the comparison is reproducible.
                    routing = {"key": None, "partition": (seq - 1 + number) % PARTITIONS}

                error, message = send(producer, TOPIC, json.dumps(event).encode(), **routing)
                if error is not None:
                    errors.inc()
                    recovering = True
                    logging.warning("publish failed user=%s seq=%s event=%s: %s", user_id, seq, event["event_id"], error)
                    stopping.wait(0.5)
                    continue

                connection.execute(
                    "UPDATE producer_progress SET confirmed_seq = %s, pending = NULL WHERE user_id = %s",
                    (seq, user_id),
                )
                published.inc()
                if recovering:
                    logging.info("publishing resumed user=%s seq=%s partition=%s offset=%s",
                                 user_id, seq, message.partition(), message.offset())
                    recovering = False
                # The three actions in one request arrive close together.
                if seq % len(ACTIONS) == 0:
                    index = (index + 1) % len(users)
                    stopping.wait(max(0, len(ACTIONS) / rate - (monotonic() - request_started)))
                    request_started = monotonic()
    finally:
        remaining = producer.flush(10)
        if remaining:
            logging.error("%s publications remained buffered; pending event is stored in PostgreSQL", remaining)


if __name__ == "__main__":
    run()
