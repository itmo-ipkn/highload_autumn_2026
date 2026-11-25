import uuid
from datetime import datetime

from sqlalchemy import CheckConstraint, String, func
from sqlalchemy.orm import Mapped, mapped_column

from consumer.db.base import Base
from models.events import ProcessStatusType


class EventProcessingResult(Base):
    __tablename__ = "event_processing_results"

    event_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    user_id: Mapped[uuid.UUID]
    seq: Mapped[int]
    pod_id: Mapped[str] = mapped_column(String(128))
    topic: Mapped[str] = mapped_column(String(128))
    partition: Mapped[int]
    event_offset: Mapped[int]
    attempt_count: Mapped[int] = mapped_column(default=1)
    error_count: Mapped[int] = mapped_column(default=0)
    status: Mapped[ProcessStatusType] = mapped_column(String(16))
    is_committed: Mapped[bool] = mapped_column(default=False)
    created_at: Mapped[datetime] = mapped_column(
        server_default=func.current_timestamp()
    )

    __table_args__ = (
        CheckConstraint("seq > 0", name="check_seq_is_positive"),
        CheckConstraint(
            "partition >= 0", name="check_partition_is_not_negative"
        ),
        CheckConstraint(
            "event_offset >= 0", name="check_event_offset_is_not_negative"
        ),
        CheckConstraint(
            "attempt_count > 0", name="check_attempt_count_is_positive"
        ),
        CheckConstraint(
            "error_count >= 0", name="check_error_count_is_not_negative"
        ),
    )
