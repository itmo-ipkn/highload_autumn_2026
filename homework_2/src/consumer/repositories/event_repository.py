import asyncio

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord

from consumer.core.logger import get_logger
from models.events import Event, EventEnvelope

logger = get_logger()


class KafkaEventRepository:
    def __init__(
        self,
        *topics: str,
        bootstrap_servers: str,
        pod_id: str,
        group_id: str | None,
    ):
        self.kafka_consumer: AIOKafkaConsumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            enable_auto_commit=False,
            group_id=group_id,
            auto_offset_reset="earliest",
        )
        self.consumer_pod_id: str = pod_id
        self.timeout_ms: int = 500

    async def start(self) -> None:
        logger.info("KafkaEventRepository - kafka consumer starting...")
        await self.kafka_consumer.start()
        logger.info("KafkaEventRepository - kafka consumer started.")

    async def get_message(self) -> EventEnvelope:
        async with asyncio.timeout(self.timeout_ms / 1000):
            message: ConsumerRecord = await self.kafka_consumer.getone()
        return EventEnvelope(
            event=Event.model_validate_json(message.value.decode("utf-8")),
            consumer_pod_id=self.consumer_pod_id,
            topic=message.topic,
            partition=message.partition,
            offset=message.offset,
        )

    async def commit(self, offset: int) -> None:
        logger.info(f"Committed offset: {offset}")
        await self.kafka_consumer.commit()

    async def stop(self) -> None:
        logger.info("KafkaEventRepository - kafka consumer stopping...")
        await self.kafka_consumer.stop()
        logger.info("KafkaEventRepository - kafka consumer stopped.")
