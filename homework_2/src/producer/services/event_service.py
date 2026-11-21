import asyncio
import random

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
        rate: int = 1,
        poison_percent: int = 2,
    ):
        self.producer: EventGenerator = event_generator
        self.repo: KafkaEventRepository = event_repository
        self.stop_event: asyncio.Event = stop_event
        self.rate: int = rate
        self.poison_percent: int = poison_percent / 100

    async def serve_forever(self):
        await self.repo.start()

        while True:
            if self.stop_event.is_set():
                logger.info("Got stop signal SET event...")
                await self.stop()
                break

            is_valid = True if random.random() > self.poison_percent else False
            event = self.producer.generate_event(is_valid=is_valid)
            await self.repo.send_event(event)

            if self.rate:
                await asyncio.sleep(1 / self.rate)

    async def stop(self):
        await self.repo.stop()
