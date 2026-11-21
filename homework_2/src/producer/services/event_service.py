import asyncio
import random

from core.config import ProjectSettings
from core.logger import get_logger
from producer.repositories.event_repository import KafkaEventRepository
from producer.utils.event_generator import EventGenerator

logger = get_logger()


class EventService:
    def __init__(
        self,
        event_generator: EventGenerator,
        event_repository: KafkaEventRepository,
        stop_event: asyncio.Event,
        settings: ProjectSettings,
    ):
        self.producer: EventGenerator = event_generator
        self.repo: KafkaEventRepository = event_repository
        self.stop_event: asyncio.Event = stop_event
        self.topic: str = settings.topic
        self.key_mode: str = settings.key_mode
        self.rate: int = settings.event_rate
        self.poison_percent: int = settings.poison_percent / 100

    async def serve_forever(self):
        await self.repo.start()

        while True:
            if self.stop_event.is_set():
                logger.info("Got stop signal SET event...")
                await self.stop()
                break

            is_valid = True if random.random() > self.poison_percent else False
            event = self.producer.generate_event(is_valid=is_valid)

            if self.key_mode:
                msg_key = event.user_id.hex.encode("utf-8")
            else:
                msg_key = None

            logger.info(f"Sending event: {event.event_id}")
            await self.repo.send_event(event, self.topic, msg_key)

            if self.rate:
                await asyncio.sleep(1 / self.rate)

    async def stop(self):
        await self.repo.stop()
