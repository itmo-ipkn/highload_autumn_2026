from aiokafka import AIOKafkaProducer

from models.events import Event
from producer.core.logger import get_logger

logger = get_logger()


class KafkaEventRepository:
    def __init__(self, bootstrap_servers: str):
        self.kafka_producer: AIOKafkaProducer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks="all",
            enable_idempotence=True,
        )

    async def start(self) -> None:
        logger.info("KafkaEventRepository - kafka producer starting...")
        await self.kafka_producer.start()
        logger.info("KafkaEventRepository - kafka producer started.")

    async def send_event(
        self,
        event: Event,
        topic: str | None = None,
        msg_key: str | None = None,
    ):
        event_data: bytes = event.model_dump_json().encode("utf-8")
        result = await self.kafka_producer.send_and_wait(
            topic, event_data, msg_key
        )
        logger.info(f"Event {event}, result: {result}")

    async def stop(self) -> None:
        logger.info("KafkaEventRepository - kafka producer stopping...")
        await self.kafka_producer.stop()
        logger.info("KafkaEventRepository - kafka producer stopped.")
