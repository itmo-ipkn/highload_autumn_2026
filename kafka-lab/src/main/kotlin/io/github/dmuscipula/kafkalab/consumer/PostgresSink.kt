package io.github.dmuscipula.kafkalab.consumer

import io.github.dmuscipula.kafkalab.postgres.openPostgresConnection
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.sql.Connection
import java.sql.SQLException
import java.util.UUID

internal data class EventToSave(
    val eventId: UUID,
    val userId: String,
    val seq: Long,
)

internal fun saveRecord(
    record: ConsumerRecord<String, String>,
    event: EventToSave,
    consumerId: Int,
) {
    val inserted = openPostgresConnection().use { connection ->
        connection.autoCommit = false

        try {
            val writeOrder = nextWriteOrder(connection, event.userId)
            val saved = insertEvent(
                connection = connection,
                record = record,
                event = event,
                consumerId = consumerId,
                writeOrder = writeOrder,
            )
            if (!saved) incrementDuplicateCounter(connection)

            connection.commit()
            saved
        } catch (failure: Exception) {
            try {
                connection.rollback()
            } catch (rollbackFailure: SQLException) {
                failure.addSuppressed(rollbackFailure)
            }
            throw failure
        }
    }

    val action = if (inserted) "Saved" else "Discarded duplicate"
    println("$action event_id=${event.eventId} topic=${record.topic()} partition=${record.partition()} offset=${record.offset()}")
}

private fun nextWriteOrder(connection: Connection, userId: String): Long {
    val sql = """
        INSERT INTO user_write_orders (user_id, last_order)
        VALUES (?, 1)
        ON CONFLICT (user_id) DO UPDATE SET last_order = user_write_orders.last_order + 1
        RETURNING last_order
    """.trimIndent()

    return connection.prepareStatement(sql).use { statement ->
        statement.setString(1, userId)
        statement.executeQuery().use { result ->
            check(result.next()) { "No write order assigned for user_id=$userId" }
            result.getLong("last_order")
        }
    }
}

private fun insertEvent(
    connection: Connection,
    record: ConsumerRecord<String, String>,
    event: EventToSave,
    consumerId: Int,
    writeOrder: Long,
): Boolean {
    val sql = """
        INSERT INTO processed_events (event_id, user_id, seq, consumer_id, topic, partition_id, record_offset, write_order)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (event_id) DO NOTHING
    """.trimIndent()

    return connection.prepareStatement(sql).use { statement ->
        statement.setObject(1, event.eventId)
        statement.setString(2, event.userId)
        statement.setLong(3, event.seq)
        statement.setInt(4, consumerId)
        statement.setString(5, record.topic())
        statement.setInt(6, record.partition())
        statement.setLong(7, record.offset())
        statement.setLong(8, writeOrder)
        statement.executeUpdate() == 1
    }
}

private fun incrementDuplicateCounter(connection: Connection) {
    val sql = """
        INSERT INTO consumer_counters (name, value)
        VALUES ('discarded_duplicates', 1)
        ON CONFLICT (name) DO UPDATE SET value = consumer_counters.value + 1
    """.trimIndent()

    connection.createStatement().use { it.executeUpdate(sql) }
}
