from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from core.logger import get_logger
from models.events import Event

logger = get_logger()


class KafkaEventRepository:
    def __init__(self, bootstrap_servers: str, default_topic: str):
        self.kafka_producer: AIOKafkaProducer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks="all",
            enable_idempotence=True,
        )
        self.topic: str = default_topic

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
        if topic is None:
            topic = self.topic

        event_data: bytes = event.model_dump_json().encode("utf-8")

        try:
            await self.kafka_producer.send_and_wait(topic, event_data, msg_key)
        except KafkaError as err:
            logger.exception(
                f"error while sending message to kafka {event=}, {err}"
            )

    async def stop(self) -> None:
        logger.info("KafkaEventRepository - kafka producer stopping...")
        await self.kafka_producer.stop()
        logger.info("KafkaEventRepository - kafka producer stopped.")
