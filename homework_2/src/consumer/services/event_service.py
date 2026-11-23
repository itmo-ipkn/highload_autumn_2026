import asyncio

from aiokafka.errors import KafkaError

from consumer.core.logger import get_logger
from consumer.db.models import EventProcessingResult
from consumer.processors.event_processor import EventProcessor
from consumer.repositories.event_repository import KafkaEventRepository
from consumer.repositories.result_repository import ProcessedEventRepository
from models.events import EventEnvelope, EventProcessResult, ProcessStatusType

logger = get_logger()


class EventService:
    def __init__(
        self,
        kafka_repo: KafkaEventRepository,
        processor: EventProcessor,
        result_repo: ProcessedEventRepository,
        stop_event: asyncio.Event,
        error_threshold: int,
    ):
        self.kafka_repo: KafkaEventRepository = kafka_repo
        self.processor: EventProcessor = processor
        self.repo: ProcessedEventRepository = result_repo
        self.stop_event: asyncio.Event = stop_event
        self.error_threshold: int = error_threshold

    async def serve_forever(self):
        await self.kafka_repo.start()

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

            await self._commit_offset(db_entry)

            if db_entry.error_count >= self.error_threshold:
                self._send_to_dlt(envelope)

    def _should_process(self, result_db: EventProcessingResult) -> bool:
        if result_db.status == ProcessStatusType.IN_PROGRESS.name:
            return True
        if result_db.status == ProcessStatusType.FAIL.name:
            return result_db.error_count < self.error_threshold
        return False

    async def _commit_offset(self, result_db: EventProcessingResult) -> None:
        # TODO: add error handling
        await self.kafka_repo.commit(result_db.event_offset)
        await self.repo.commit_offset(result_db.event_id)

    def _send_to_dlt(self, envelope: EventEnvelope) -> None:
        logger.info(f"Sending event {envelope.event.event_id} to DLT.")

    async def stop(self):
        await self.kafka_repo.stop()
