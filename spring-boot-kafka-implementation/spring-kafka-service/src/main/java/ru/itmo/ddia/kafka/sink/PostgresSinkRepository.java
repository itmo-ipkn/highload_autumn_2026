package ru.itmo.ddia.kafka.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import ru.itmo.ddia.kafka.model.EventMessage;

import java.sql.Timestamp;

@Repository
@ConditionalOnProperty(name = "app.role", havingValue = "consumer")
public class PostgresSinkRepository {

    private static final Logger log = LoggerFactory.getLogger(PostgresSinkRepository.class);

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public PostgresSinkRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Performs an idempotent insert into sink_events keyed by event_id.
     * If event_id already exists (rowsAffected == 0), increments the deduplication
     * counter in dedup_stats and returns false.
     */
    @Transactional
    public boolean saveIdempotent(
            EventMessage event,
            String processorPod,
            String topic,
            int partition,
            long offset
    ) {
        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(event.payload());
        } catch (JsonProcessingException e) {
            payloadJson = "{}";
        }

        int insertedRows = jdbcTemplate.update("""
                INSERT INTO sink_events (
                    event_id, user_id, seq, event_type, processor_pod,
                    topic, partition, kafka_offset, produced_at, payload
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
                ON CONFLICT (event_id) DO NOTHING
                """,
                event.eventId(),
                event.userId(),
                event.seq(),
                event.type(),
                processorPod,
                topic,
                partition,
                offset,
                Timestamp.from(event.producedAt()),
                payloadJson
        );

        if (insertedRows == 0) {
            jdbcTemplate.update("""
                    UPDATE dedup_stats
                    SET rejected_duplicates = rejected_duplicates + 1,
                        updated_at = NOW()
                    WHERE stat_key = 'global'
                    """);
            log.warn("[{}] DEDUPLICATED duplicate delivery: event_id={}, user_id={}, seq={}, {}[p={}@offset={}]",
                    processorPod, event.eventId(), event.userId(), event.seq(), topic, partition, offset);
            return false;
        }

        return true;
    }
}
