import asyncio

from aiokafka.errors import KafkaError

from producer.core.config import AppSettings
from producer.core.logger import get_logger
from producer.repositories.event_repository import KafkaEventRepository
from producer.utils.event_generator import EventGenerator

logger = get_logger()


class EventService:
    def __init__(
        self,
        event_generator: EventGenerator,
        event_repository: KafkaEventRepository,
        stop_event: asyncio.Event,
        settings: AppSettings,
    ):
        self.producer: EventGenerator = event_generator
        self.repo: KafkaEventRepository = event_repository
        self.stop_event: asyncio.Event = stop_event
        self.topic: str = settings.topic
        self.key_mode: str = settings.producer_settings.key_mode
        self.rate: int = settings.producer_settings.event_rate

    async def serve_forever(self):
        await self.repo.start()

        while True:
            if self.stop_event.is_set():
                logger.info("Got stop signal SET event...")
                await self.stop()
                break

            event = self.producer.generate_event()

            if self.key_mode:
                msg_key = event.user_id.hex.encode("utf-8")
            else:
                msg_key = None

            logger.info(f"Sending event: {event.event_id}")

            try:
                await self.repo.send_event(event, self.topic, msg_key)
            except KafkaError as kafka_err:
                logger.exception(
                    f"Error occured while sending message to kafka {event}, {kafka_err}"
                )

            if self.rate:
                await asyncio.sleep(1 / self.rate)

    async def stop(self):
        await self.repo.stop()
