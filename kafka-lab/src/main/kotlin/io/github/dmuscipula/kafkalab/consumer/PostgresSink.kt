package io.github.dmuscipula.kafkalab.consumer

import org.apache.kafka.clients.consumer.ConsumerRecord
import java.sql.DriverManager
import java.util.UUID

internal data class EventToSave(val eventId: UUID, val userId: String, val seq: Long)

internal fun saveRecord(record: ConsumerRecord<String, String>, event: EventToSave, consumerId: Int) {
    val url = "jdbc:postgresql://postgres:5432/kafka_lab"
    val password = System.getenv("POSTGRES_PASSWORD") ?: "kafka-lab-local"

    val duplicate = DriverManager.getConnection(url, "kafka_lab", password).use { connection ->
        connection.prepareStatement(saveEventSql).use { statement ->
            statement.setObject(1, event.eventId)
            statement.setString(2, event.userId)
            statement.setLong(3, event.seq)
            statement.setInt(4, consumerId)
            statement.setString(5, record.topic())
            statement.setInt(6, record.partition())
            statement.setLong(7, record.offset())
            statement.executeUpdate() == 1
        }
    }

    val action = if (duplicate) "Discarded duplicate" else "Saved"
    println("$action event_id=${event.eventId} topic=${record.topic()} partition=${record.partition()} offset=${record.offset()}")
}

private val saveEventSql = """
    WITH inserted_event AS (
        INSERT INTO processed_events (event_id, user_id, seq, consumer_id, topic, partition_id, record_offset)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (event_id) DO NOTHING
        RETURNING 1
    )
    INSERT INTO consumer_counters (name, value)
    SELECT 'discarded_duplicates', 1
    WHERE NOT EXISTS (SELECT 1 FROM inserted_event)
    ON CONFLICT (name) DO UPDATE SET value = consumer_counters.value + 1
""".trimIndent()
