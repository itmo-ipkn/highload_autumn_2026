import uuid
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Index, Integer, Text, UniqueConstraint, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class ProcessedEvent(Base):
    """Результат обработки события."""

    __tablename__ = "processed_events"
    __table_args__ = (
        # Дедупликация: уникальность event_id гарантирует сама БД.
        UniqueConstraint("event_id", name="processed_events_event_id_key"),
        Index("processed_events_user_idx", "user_id", "id"),
    )

    # Монотонный номер записи: по нему verify определяет, что было записано раньше, а что позже.
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_id: Mapped[uuid.UUID]
    user_id: Mapped[str] = mapped_column(Text)
    seq: Mapped[int] = mapped_column(BigInteger)
    type: Mapped[str] = mapped_column(Text)
    pod: Mapped[str] = mapped_column(Text)
    topic: Mapped[str] = mapped_column(Text)
    partition: Mapped[int] = mapped_column(Integer)
    offset: Mapped[int] = mapped_column(BigInteger)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("clock_timestamp()")
    )


class PodCounter(Base):
    """Счётчик отброшенных дублей, по строке на под-консьюмер. verify печатает сумму."""

    __tablename__ = "pod_counters"

    pod: Mapped[str] = mapped_column(Text, primary_key=True)
    dedup_dropped: Mapped[int] = mapped_column(BigInteger, server_default=text("0"))
