import asyncio
import signal

from core.config import settings
from core.logger import get_logger
from core.signal_handlers import sigint_handler, sigterm_handler
from producer.repositories.event_repository import KafkaEventRepository
from producer.services.event_service import EventService
from producer.utils.event_generator import EventGenerator

logger = get_logger()


async def main():
    logger.info("Starting...")

    generator = EventGenerator()
    repo = KafkaEventRepository(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        default_topic=settings.kafka.topic,
    )
    stop_event = asyncio.Event()
    event_service = EventService(
        event_generator=generator,
        event_repository=repo,
        stop_event=stop_event,
        rate=settings.project.event_rate,
        poison_percent=settings.project.poison_percent,
    )

    termination_handler = sigterm_handler(stop_event)
    interrupt_handler = sigint_handler(stop_event)

    signal.signal(signal.SIGTERM, termination_handler)
    signal.signal(signal.SIGINT, interrupt_handler)

    await event_service.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
