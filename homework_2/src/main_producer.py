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
    )
    stop_event = asyncio.Event()
    event_service = EventService(
        event_generator=generator,
        event_repository=repo,
        stop_event=stop_event,
        settings=settings.project,
    )

    termination_handler = sigterm_handler(stop_event)
    interrupt_handler = sigint_handler(stop_event)

    signal.signal(signal.SIGTERM, termination_handler)
    signal.signal(signal.SIGINT, interrupt_handler)

    try:
        await event_service.serve_forever()
    except Exception as err:
        logger.exception(f"Exception occured: {err}")

        if not stop_event.is_set():
            await event_service.stop()


if __name__ == "__main__":
    asyncio.run(main())
