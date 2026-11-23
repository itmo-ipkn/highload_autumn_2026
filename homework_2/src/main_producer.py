import asyncio
import signal

from producer.core.config import settings
from producer.core.logger import get_logger
from producer.repositories.event_repository import KafkaEventRepository
from producer.services.event_service import EventService
from producer.utils.event_generator import EventGenerator
from producer.utils.user_service import UserService

logger = get_logger()


async def main():
    logger.info("Starting...")

    user_service = UserService()
    generator = EventGenerator(
        poison_percent=settings.producer_settings.poison_percent,
        user_service=user_service,
    )
    repo = KafkaEventRepository(
        bootstrap_servers=settings.bootstrap_servers,
    )
    stop_event = asyncio.Event()
    event_service = EventService(
        event_generator=generator,
        event_repository=repo,
        stop_event=stop_event,
        settings=settings,
    )

    loop = asyncio.get_running_loop()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    try:
        await event_service.serve_forever()
    except Exception as err:
        logger.exception(f"Exception occured: {err}")

        if not stop_event.is_set():
            await event_service.stop()


if __name__ == "__main__":
    asyncio.run(main())
