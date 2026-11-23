import asyncio

from consumer.core.config import settings
from consumer.core.logger import get_logger
from models.events import EventEnvelope, EventProcessResult, ProcessStatusType

logger = get_logger()


class EventProcessor:
    """Emulate message processing."""

    async def process(
        self, event_envelope: EventEnvelope
    ) -> EventProcessResult:
        message = event_envelope.event.payload["message"]

        if message == "Valid event":
            status = ProcessStatusType.SUCCESS
            error_reason = ""
        else:
            status = ProcessStatusType.FAIL
            error_reason = "Unprocessable content"

        await asyncio.sleep(settings.consumer_settings.delay / 1000)

        logger.info(f"Processed event {event_envelope.event.event_id}")
        return EventProcessResult(
            event_id=event_envelope.event.event_id,
            user_id=event_envelope.event.user_id,
            seq=event_envelope.event.seq,
            consumer_pod_id=event_envelope.consumer_pod_id,
            topic=event_envelope.topic,
            partition=event_envelope.partition,
            offset=event_envelope.offset,
            status=status,
            error_reason=error_reason,
        )
