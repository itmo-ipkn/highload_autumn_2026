import uuid

from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from consumer.core.logger import get_logger
from consumer.db.models import EventProcessingResult
from models.events import EventEnvelope, EventProcessResult, ProcessStatusType

logger = get_logger()


class ProcessedEventRepository:
    def __init__(self, db_session: AsyncSession) -> None:
        self.db_session: AsyncSession = db_session

    async def create(self, envelope: EventEnvelope) -> EventProcessingResult:
        stmt = (
            insert(EventProcessingResult)
            .values(
                event_id=envelope.event.event_id,
                user_id=envelope.event.user_id,
                seq=envelope.event.seq,
                pod_id=envelope.consumer_pod_id,
                topic=envelope.topic,
                partition=envelope.partition,
                event_offset=envelope.offset,
                attempt_count=1,
                status=ProcessStatusType.IN_PROGRESS,
            )
            .on_conflict_do_update(
                index_elements=[EventProcessingResult.event_id],
                set_={
                    "pod_id": envelope.consumer_pod_id,
                    "attempt_count": EventProcessingResult.attempt_count + 1,
                },
            )
            .returning(EventProcessingResult)
        )

        async with self.db_session() as session:
            db_result = await session.execute(stmt)
            await session.commit()

        return db_result.scalar_one()

    async def update(
        self, process_result: EventProcessResult
    ) -> EventProcessingResult:
        update_values: dict = {"status": process_result.status}

        if process_result.status == ProcessStatusType.FAIL:
            update_values.update(
                {
                    "error_count": EventProcessingResult.error_count + 1,
                }
            )

        stmt = (
            update(EventProcessingResult)
            .where(
                EventProcessingResult.event_id == process_result.event_id,
            )
            .values(**update_values)
            .returning(EventProcessingResult)
        )

        async with self.db_session() as session:
            db_result = await session.execute(stmt)
            await session.commit()

        return db_result.scalar_one()

    async def commit_offset(
        self, event_id: uuid.UUID
    ) -> EventProcessingResult:
        stmt = (
            update(EventProcessingResult)
            .where(
                EventProcessingResult.event_id == event_id,
            )
            .values(
                is_committed=True,
            )
            .returning(EventProcessingResult)
        )

        async with self.db_session() as session:
            db_result = await session.execute(stmt)
            await session.commit()

        return db_result.scalar_one()
