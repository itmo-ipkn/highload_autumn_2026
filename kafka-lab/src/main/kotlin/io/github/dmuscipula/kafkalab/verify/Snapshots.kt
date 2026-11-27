package io.github.dmuscipula.kafkalab.verify

import io.github.dmuscipula.kafkalab.postgres.openPostgresConnection
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import tools.jackson.databind.json.JsonMapper
import java.sql.Connection
import java.time.Duration

private const val DEAD_LETTER_TOPIC = "events.dlt"
private val readTimeout = Duration.ofSeconds(30)
private val mapper = JsonMapper.builder().build()

internal data class EventSequence(val userId: String, val seq: Long) {
    init {
        require(seq > 0) { "Invalid seq=$seq for user_id=$userId" }
    }
}

internal data class StoredEvent(val event: EventSequence, val writeOrder: Long?)
internal data class SinkSnapshot(val events: List<StoredEvent>, val discardedDuplicates: Long)

internal fun readSinkSnapshot(): SinkSnapshot = openPostgresConnection().use { connection ->
    connection.isReadOnly = true
    connection.transactionIsolation = Connection.TRANSACTION_REPEATABLE_READ
    connection.autoCommit = false

    val events = connection.createStatement().use { statement ->
        statement.executeQuery(
            "SELECT user_id, seq, write_order FROM processed_events ORDER BY user_id, write_order NULLS LAST",
        ).use { result ->
            buildList {
                while (result.next()) {
                    val event = EventSequence(result.getString("user_id"), result.getLong("seq"))
                    val writeOrder = result.getLong("write_order").takeUnless { result.wasNull() }
                    add(StoredEvent(event, writeOrder))
                }
            }
        }
    }
    val discardedDuplicates = connection.createStatement().use { statement ->
        statement.executeQuery("SELECT value FROM consumer_counters WHERE name = 'discarded_duplicates'").use { result ->
            check(result.next()) { "Duplicate counter is missing" }
            result.getLong("value")
        }
    }
    connection.commit()
    SinkSnapshot(events, discardedDuplicates)
}

internal fun readDeadLetterSnapshot(): Set<EventSequence> {
    val bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "kafka1:9092,kafka2:9092,kafka3:9092"
    val config = mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "none",
        ConsumerConfig.ISOLATION_LEVEL_CONFIG to "read_committed",
    )

    KafkaConsumer(config, StringDeserializer(), StringDeserializer()).use { consumer ->
        val partitions = consumer.partitionsFor(DEAD_LETTER_TOPIC, readTimeout)
            .map { TopicPartition(it.topic(), it.partition()) }
        check(partitions.isNotEmpty()) { "No partitions found for $DEAD_LETTER_TOPIC" }

        val startOffsets = consumer.beginningOffsets(partitions, readTimeout)
        val endOffsets = consumer.endOffsets(partitions, readTimeout)
        val events = mutableSetOf<EventSequence>()
        val startedAt = System.nanoTime()
        for (partition in partitions) {
            val start = startOffsets.getValue(partition)
            val end = endOffsets.getValue(partition)
            if (start >= end) continue

            consumer.assign(listOf(partition))
            consumer.seek(partition, start)
            while (consumer.position(partition) < end) {
                check(System.nanoTime() - startedAt < readTimeout.toNanos()) { "Timed out reading $partition" }
                for (record in consumer.poll(Duration.ofMillis(250))) {
                    if (record.offset() >= end) break

                    val event = mapper.readTree(record.value())
                    events.add(EventSequence(event.required("user_id").asString(), event.required("seq").asLong()))
                }
            }
        }
        return events
    }
}
