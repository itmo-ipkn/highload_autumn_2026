package itmo.ddia.hw2.persistence;

import itmo.ddia.hw2.model.Event;
import itmo.ddia.hw2.model.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Repository
public interface SinkRepository extends JpaRepository<ProcessedEvent, UUID> {

    @Modifying
    @Query(value = """
            INSERT INTO processed_events (
                event_id, user_id, seq, event_type, consumer_pod_id,
                topic, partition_id, offset_id
            ) VALUES (
                :eventId, :userId, :sequence, :eventType, :consumerPodId,
                :topic, :partition, :recordOffset
            )
            ON CONFLICT (event_id) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(
            @Param("eventId") UUID eventId,
            @Param("userId") String userId,
            @Param("sequence") long sequence,
            @Param("eventType") String eventType,
            @Param("consumerPodId") String consumerPodId,
            @Param("topic") String topic,
            @Param("partition") int partition,
            @Param("recordOffset") long offset
    );

    @Modifying
    @Query(value = """
            UPDATE duplicate_delivery_stats
            SET dropped_duplicates = dropped_duplicates + 1
            WHERE id = 1
            """, nativeQuery = true)
    int incrementDroppedDuplicates();

    @Transactional
    default boolean saveEvent(
            Event event,
            String consumerPodId,
            String topic,
            int partition,
            long offset
    ) {
        int inserted = insertIfAbsent(
                UUID.fromString(event.event_id()),
                event.user_id(),
                event.seq(),
                event.type(),
                consumerPodId,
                topic,
                partition,
                offset
        );

        if (inserted == 0) {
            incrementDroppedDuplicates();
            return false;
        }

        return true;
    }
}
