from uuid import UUID

import psycopg


MAX_ATTEMPTS = 3


class Sink:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self.connection: psycopg.Connection | None = None

    def connect(self) -> psycopg.Connection:
        if self.connection is None or self.connection.closed:
            self.connection = psycopg.connect(
                self.database_url,
                autocommit=True,
                connect_timeout=5,
                options="-c statement_timeout=5000",
            )
        return self.connection

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()

    def store_event(
        self,
        event_id: UUID,
        user_id: str,
        seq: int,
        consumer_id: str,
        source: tuple[str, int, int],
    ) -> bool:
        connection = self.connect()
        with connection.transaction(), connection.cursor() as cursor:
            # Hold the lock until commit so write_order reflects durable write order.
            cursor.execute("SELECT pg_advisory_xact_lock(2026, 2)")
            cursor.execute(
                """
                INSERT INTO processed_events
                    (event_id, user_id, seq, consumer_id, source_topic,
                     source_partition, source_offset)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                """,
                (event_id, user_id, seq, consumer_id, *source),
            )
            if cursor.rowcount == 1:
                return True
            cursor.execute(
                """
                UPDATE processed_events
                SET duplicates = duplicates + 1
                WHERE event_id = %s
                """,
                (event_id,),
            )
        return False

    def record_failure(self, source: tuple[str, int, int], error: str) -> int:
        with self.connect().cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO processing_failures
                    (source_topic, source_partition, source_offset, attempts, last_error)
                VALUES (%s, %s, %s, 1, %s)
                ON CONFLICT (source_topic, source_partition, source_offset) DO UPDATE
                SET attempts = LEAST(processing_failures.attempts + 1, %s),
                    last_error = EXCLUDED.last_error
                RETURNING attempts
                """,
                (*source, error, MAX_ATTEMPTS),
            )
            return cursor.fetchone()[0]
