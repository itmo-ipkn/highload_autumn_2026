package org.highload.repositories;

import org.highload.entities.Event;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.UUID;

public interface EventRepository
        extends JpaRepository<Event, UUID> {

    @Modifying
    @Query(value = """
            INSERT INTO events (
                event_id,
                user_id,
                seq,
                pod_id,
                topic,
                partition_id,
                "offset",
                type,
                produced_at,
                payload
            )
            SELECT
                :eventId,
                :userId,
                :seq,
                :podId,
                :topic,
                :partitionId,
                :offset,
                :type,
                :producedAt,
                CAST(:payload AS jsonb)
            FROM pg_advisory_xact_lock(:userId)
            ON CONFLICT (event_id) DO NOTHING
    """, nativeQuery = true)
    int insertIfAbsent(
            @Param("eventId") UUID eventId,
            @Param("userId") Long userId,
            @Param("seq") Long seq,
            @Param("podId") Integer podId,
            @Param("topic") String topic,
            @Param("partitionId") Integer partitionId,
            @Param("offset") Long offset,
            @Param("type") String type,
            @Param("producedAt") Instant producedAt,
            @Param("payload") String payload
    );
}
