from aiokafka import AIOKafkaProducer

from consumer.core.logger import get_logger
from models.events import EventEnvelope

logger = get_logger()


class KafkaDLTRepository:
    def __init__(self, bootstrap_servers: str, dlt_topic: str):
        self.kafka_producer: AIOKafkaProducer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks="all",
            enable_idempotence=True,
        )
        self.dlt_topic: str = dlt_topic

    async def start(self) -> None:
        logger.info("KafkaDLTRepository - kafka producer starting...")
        await self.kafka_producer.start()
        logger.info("KafkaDLTRepository - kafka producer started.")

    async def send(self, envelope: EventEnvelope, reason: str) -> None:
        event_data: bytes = envelope.event.model_dump_json().encode("utf-8")
        headers: list[tuple[str, bytes]] = [
            ("source_topic", envelope.topic.encode("utf-8")),
            ("source_partition", str(envelope.partition).encode("utf-8")),
            ("source_offset", str(envelope.offset).encode("utf-8")),
            ("reason", reason.encode("utf-8")),
        ]
        await self.kafka_producer.send_and_wait(
            topic=self.dlt_topic,
            value=event_data,
            headers=headers,
        )
        logger.info(f"Event {envelope.event}, sent to DLT.")

    async def stop(self) -> None:
        logger.info("KafkaDLTRepository - kafka producer stopping...")
        await self.kafka_producer.stop()
        logger.info("KafkaDLTRepository - kafka producer stopped.")
