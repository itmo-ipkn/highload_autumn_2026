import asyncio
import uuid

from aiokafka.errors import CommitFailedError, KafkaError
from sqlalchemy.exc import SQLAlchemyError

from consumer.core.config import settings
from consumer.core.logger import get_logger
from consumer.db.models import EventProcessingResult
from consumer.processors.event_processor import EventProcessor
from consumer.repositories.event_repository import KafkaEventRepository
from consumer.repositories.result_repository import ProcessedEventRepository
from consumer.services.dlt_service import KafkaDLTRepository
from models.events import EventEnvelope, EventProcessResult, ProcessStatusType

logger = get_logger()

PROCESS_RESULT_SANTINEL = EventProcessResult(
    event_id=uuid.UUID('00000000-0000-4000-0000-000000000000'),
    user_id=uuid.UUID('00000000-0000-4000-0000-000000000000'),
    seq=0,
    consumer_pod_id=settings.consumer_settings.pod_id,
    topic=settings.topic,
    partition=0,
    offset=0,
    status=ProcessStatusType.FAIL,
    error_reason="Santinel",
)


class EventService:
    def __init__(
        self,
        kafka_repo: KafkaEventRepository,
        kafka_dlt_repo: KafkaDLTRepository,
        processor: EventProcessor,
        result_repo: ProcessedEventRepository,
        stop_event: asyncio.Event,
        error_threshold: int,
    ):
        self.kafka_repo: KafkaEventRepository = kafka_repo
        self.kafka_dlt_repo: KafkaDLTRepository = kafka_dlt_repo
        self.processor: EventProcessor = processor
        self.repo: ProcessedEventRepository = result_repo
        self.stop_event: asyncio.Event = stop_event
        self.error_threshold: int = error_threshold

    async def serve_forever(self):
        await self.kafka_repo.start()
        await self.kafka_dlt_repo.start()

        while True:
            if self.stop_event.is_set():
                logger.info("Got stop signal SET event...")
                await self.stop()
                break

            try:
                envelope: EventEnvelope = await self.kafka_repo.get_message()
            except asyncio.TimeoutError:
                continue
            except KafkaError as kafka_err:
                logger.exception(f"Kafka error occured: {kafka_err}")
                continue

            try:
                db_entry = await self.repo.create(envelope)
            except Exception as repo_err:
                logger.exception(
                    f"Event repository error occured for create: {repo_err}"
                )
                continue

            process_result: EventProcessResult = PROCESS_RESULT_SANTINEL

            while self._should_process(db_entry):
                try:
                    process_result = await self.processor.process(envelope)
                except Exception as processor_err:
                    logger.exception(
                        f"Event processor error occured: {processor_err}"
                    )
                    continue

                try:
                    db_entry = await self.repo.update(process_result)
                except Exception as repo_err:
                    logger.exception(
                        f"Event repository error occured for update: {repo_err}"
                    )

            try:
                await self._commit_offset(db_entry)
            except CommitFailedError as kafka_commit_err:
                logger.exception(
                    f"Kakfa commit error occured, err: {kafka_commit_err}"
                )
                continue
            except SQLAlchemyError as sql_err:
                logger.exception(
                    f"SQLAlchemy error occured, event: {db_entry.event_id}, err: {sql_err}"
                )

            if db_entry.error_count >= self.error_threshold:
                await self._send_to_dlt(envelope, process_result.error_reason)

    def _should_process(self, result_db: EventProcessingResult) -> bool:
        if result_db.status == ProcessStatusType.IN_PROGRESS.name:
            return True
        if result_db.status == ProcessStatusType.FAIL.name:
            return result_db.error_count < self.error_threshold
        return False

    async def _commit_offset(self, result_db: EventProcessingResult) -> None:
        await self.repo.commit_offset(result_db.event_id)
        await self.kafka_repo.commit(result_db.event_offset)

    async def _send_to_dlt(self, envelope: EventEnvelope, reason: str) -> None:
        # TODO: add error handling
        logger.info(f"Sending event {envelope.event.event_id} to DLT.")
        await self.kafka_dlt_repo.send(envelope, reason)

    async def stop(self):
        await self.kafka_repo.stop()
        await self.kafka_dlt_repo.stop()
