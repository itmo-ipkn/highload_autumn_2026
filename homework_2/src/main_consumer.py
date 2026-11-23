import asyncio
import signal

from consumer.core.config import settings
from consumer.core.logger import get_logger
from consumer.core.postgres import async_session_local
from consumer.processors.event_processor import EventProcessor
from consumer.repositories.event_repository import KafkaEventRepository
from consumer.repositories.result_repository import ProcessedEventRepository
from consumer.services.event_service import EventService
from core_common.signal_handlers import sigint_handler, sigterm_handler

logger = get_logger()


async def main():
    logger.info("Starting...")

    repo = KafkaEventRepository(
        settings.topic,
        bootstrap_servers=settings.bootstrap_servers,
        pod_id=settings.consumer_settings.pod_id,
        group_id=settings.consumer_settings.group_id,
    )
    event_processor = EventProcessor()
    result_repo = ProcessedEventRepository(async_session_local)
    stop_event = asyncio.Event()
    event_service = EventService(
        kafka_repo=repo,
        processor=event_processor,
        result_repo=result_repo,
        stop_event=stop_event,
        error_threshold=settings.consumer_settings.error_threshold,
    )

    termination_handler = sigterm_handler(stop_event, logger)
    interrupt_handler = sigint_handler(stop_event, logger)

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
