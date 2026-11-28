import logging
import os
import signal
from threading import Event

import psycopg
from confluent_kafka import Producer

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
TOPIC = "events"
DEAD_LETTER_TOPIC = "events.dlt"
PARTITIONS = 3
GROUP = "events-processor"


def configure_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def stop_event():
    stopping = Event()
    signal.signal(signal.SIGTERM, lambda *_: stopping.set())
    signal.signal(signal.SIGINT, lambda *_: stopping.set())
    return stopping


def database():
    return psycopg.connect(os.environ["DATABASE_URL"], autocommit=True, connect_timeout=5)


def kafka_producer(client_id):
    return Producer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "client.id": client_id,
            "acks": "all",
            "enable.idempotence": True,
            "partitioner": "murmur2_random",
            "request.timeout.ms": 4000,
            "message.timeout.ms": 8000,
        }
    )


def send(producer, topic, value, **kwargs):
    delivery = []

    def reported(error, message):
        delivery.append((error, message))

    producer.produce(topic, value=value, on_delivery=reported, **kwargs)
    while not delivery:
        producer.poll(0.1)
    return delivery[0]
