import logging

from confluent_kafka import ConsumerGroupTopicPartitions, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec
from prometheus_client import Gauge, start_http_server

from .settings import BOOTSTRAP, GROUP, PARTITIONS, TOPIC, configure_logging, stop_event

LAG = Gauge("netinventory_consumer_lag", "Records behind per partition", ("partition",))
COMMITTED = Gauge("netinventory_committed_offset", "Consumer group next position", ("partition",))
BROKERS = Gauge("netinventory_live_brokers", "Brokers reported by Kafka metadata")
SCRAPE_OK = Gauge("netinventory_kafka_scrape_ok", "Whether the last Kafka metadata and offsets pull succeeded")


def update(admin):
    metadata = admin.list_topics(timeout=2)
    BROKERS.set(len(metadata.brokers))
    partitions = [TopicPartition(TOPIC, i) for i in range(PARTITIONS)]
    group = admin.list_consumer_group_offsets(
        [ConsumerGroupTopicPartitions(GROUP, partitions)], request_timeout=2
    )[GROUP].result(3)
    committed = {item.partition: max(0, item.offset) for item in group.topic_partitions if not item.error}
    latest = admin.list_offsets({item: OffsetSpec.latest() for item in partitions}, request_timeout=2)
    for position, future in latest.items():
        if position.partition not in committed:
            raise RuntimeError(f"group offset unavailable for partition {position.partition}")
        offset = committed[position.partition]
        end = future.result(3).offset
        COMMITTED.labels(position.partition).set(offset)
        LAG.labels(position.partition).set(max(0, end - offset))


def run():
    configure_logging()
    stopping = stop_event()
    start_http_server(9100)
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP, "client.id": "queue-monitor"})
    while not stopping.is_set():
        try:
            update(admin)
        except (KafkaException, RuntimeError, TimeoutError) as error:
            SCRAPE_OK.set(0)
            logging.warning("Kafka metrics unavailable: %s", error)
        else:
            SCRAPE_OK.set(1)
        stopping.wait(2)


if __name__ == "__main__":
    run()
