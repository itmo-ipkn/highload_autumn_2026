"""Sequential at-least-once processing with an explicit completed frontier."""
import json
import os
import time

import psycopg
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

from src.config import Config, log, setup_logging, stop_event
from src.events import PoisonEvent, process_event
from src.sink import Sink


class Offsets:
    def __init__(self, consumer, identity):
        self.consumer = consumer
        self.identity = identity
        self.ready = {}

    def done(self, message):
        self.ready[(message.topic(), message.partition())] = message.offset() + 1

    def commit(self, keys=None):
        keys = set(self.ready) if keys is None else set(keys) & self.ready.keys()
        positions = [TopicPartition(t, p, self.ready[t, p]) for t, p in keys]
        if not positions:
            return
        try:
            result = self.consumer.commit(offsets=positions, asynchronous=False)
            for part in result:
                if part.error:
                    log("commit_error", consumer=self.identity, partition=part.partition, error=str(part.error))
                else:
                    key = (part.topic, part.partition)
                    if self.ready.get(key) == part.offset:
                        self.ready.pop(key)
                    log("offset_committed", consumer=self.identity, partition=part.partition, next_offset=part.offset)
        except KafkaException as exc:
            # Leave completed positions pending; a new owner may safely replay them.
            log("commit_error", consumer=self.identity, error=str(exc))

    def assigned(self, consumer, partitions):
        log("partitions_assigned", consumer=self.identity,
            partitions=[p.partition for p in partitions])

    def revoked(self, consumer, partitions):
        keys = {(p.topic, p.partition) for p in partitions}
        log("partitions_revoked", consumer=self.identity, partitions=[p.partition for p in partitions])
        self.commit(keys)
        for key in keys:
            self.ready.pop(key, None)

    def lost(self, consumer, partitions):
        log("partitions_lost", consumer=self.identity, partitions=[p.partition for p in partitions])
        for p in partitions:
            self.ready.pop((p.topic, p.partition), None)


def send_dlt(producer, message, error, identity):
    result = []
    headers = list(message.headers() or []) + [
        ("error", str(error).encode()),
        ("source_topic", message.topic().encode()),
        ("source_partition", str(message.partition()).encode()),
        ("source_offset", str(message.offset()).encode()),
        ("attempts", b"3"),
    ]
    producer.produce(
        "events.dlt", key=message.key(), value=message.value(), headers=headers,
        on_delivery=lambda err, msg: result.append(err),
    )
    remaining = producer.flush(125)
    if remaining or not result or result[0] is not None:
        raise RuntimeError(f"DLT delivery not confirmed: {result}, remaining={remaining}")
    log("dlt_delivered", consumer=identity, partition=message.partition(), offset=message.offset())


def main():
    setup_logging()
    cfg = Config.load()
    identity = os.environ["CONSUMER_ID"]
    stop = stop_event()
    consumer = Consumer({
        "bootstrap.servers": cfg.brokers,
        "group.id": "events-processor", "client.id": f"consumer-{identity}",
        "enable.auto.commit": False, "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
        "partition.assignment.strategy": "range",
        "session.timeout.ms": 10000, "heartbeat.interval.ms": 3000,
        "max.poll.interval.ms": 300000, "socket.timeout.ms": 10000,
        "allow.auto.create.topics": False,
    })
    dlt = Producer({
        "bootstrap.servers": cfg.brokers, "client.id": f"dlt-{identity}",
        "acks": "all", "enable.idempotence": True,
        "delivery.timeout.ms": 120000, "request.timeout.ms": 10000,
    })
    sink = Sink()
    offsets = Offsets(consumer, identity)
    consumer.subscribe(["events"], on_assign=offsets.assigned, on_revoke=offsets.revoked, on_lost=offsets.lost)
    try:
        while not stop.is_set():
            message = consumer.poll(0.5)
            if message is None:
                offsets.commit()
                continue
            if message.error():
                log("consume_error", consumer=identity, error=str(message.error()))
                continue
            event = json.loads(message.value())
            log("processing", consumer=identity, event_id=event["event_id"],
                partition=message.partition(), offset=message.offset(), seq=event["seq"])
            poison = None
            for attempt in range(1, 4):
                time.sleep(cfg.delay_ms / 1000)
                try:
                    process_event(event)
                    poison = None
                    break
                except PoisonEvent as exc:
                    poison = exc
                    log("poison_attempt", consumer=identity, event_id=event["event_id"], attempt=attempt, error=str(exc))
            if poison is not None:
                send_dlt(dlt, message, poison, identity)
            else:
                deadline = time.monotonic() + 45
                while True:
                    try:
                        inserted = sink.store(event, identity, message.topic(), message.partition(), message.offset())
                        break
                    except psycopg.Error as exc:
                        sink.close()
                        log("sink_error", consumer=identity, error=str(exc))
                        if stop.is_set() or time.monotonic() >= deadline:
                            raise
                        stop.wait(1)
                log("sink_written" if inserted else "duplicate_discarded", consumer=identity,
                    event_id=event["event_id"], partition=message.partition(), offset=message.offset())
            offsets.done(message)
            if cfg.before_commit_ms:
                log("before_commit_pause", consumer=identity, partition=message.partition(), offset=message.offset())
                stop.wait(cfg.before_commit_ms / 1000)
            offsets.commit()
    finally:
        offsets.commit()
        consumer.close()
        sink.close()
        log("consumer_stopped", consumer=identity)


if __name__ == "__main__":
    main()
