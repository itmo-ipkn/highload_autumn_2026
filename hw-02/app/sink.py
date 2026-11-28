import uuid

from sqlalchemy import create_engine, update
from sqlalchemy.dialects.postgresql import insert

from models import PodCounter, ProcessedEvent


class Sink:
    def __init__(self, dsn: str, pod: str):
        self.pod = pod
        # pool_pre_ping: после потери соединения с sink следующая попытка возьмёт новое
        self.engine = create_engine(dsn, pool_size=1, max_overflow=0, pool_pre_ping=True)
        with self.engine.begin() as conn:
            conn.execute(insert(PodCounter).values(pod=pod).on_conflict_do_nothing())

    def write(self, event: dict, topic: str, partition: int, offset: int) -> bool:
        """Идемпотентно записать результат обработки события.

        True — строка записана; False — такой event_id уже был (дубль отброшен).
        Арбитр — UNIQUE(event_id): при гонке двух консьюмеров вставит только один.
        Вставка и инкремент счётчика дублей идут в одной транзакции.
        """
        stmt = (
            insert(ProcessedEvent)
            .values(
                event_id=uuid.UUID(event["event_id"]),
                user_id=event["user_id"],
                seq=event["seq"],
                type=event["type"],
                pod=self.pod,
                topic=topic,
                partition=partition,
                offset=offset,
            )
            .on_conflict_do_nothing(index_elements=[ProcessedEvent.event_id])
            # при конфликте строка не вставляется и RETURNING ничего не возвращает
            .returning(ProcessedEvent.id)
        )
        with self.engine.begin() as conn:
            inserted = conn.execute(stmt).scalar_one_or_none() is not None
            if not inserted:
                conn.execute(
                    update(PodCounter)
                    .where(PodCounter.pod == self.pod)
                    .values(dedup_dropped=PodCounter.dedup_dropped + 1)
                )
        return inserted

    def close(self):
        self.engine.dispose()
