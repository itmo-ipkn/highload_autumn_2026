package io.github.dmuscipula.kafkalab

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import tools.jackson.databind.json.JsonMapper
import java.sql.DriverManager
import java.time.Duration
import java.util.UUID

private val consumerMapper = JsonMapper.builder().build()

object Consumer {
    @JvmStatic
    fun main(args: Array<String>) {
        require(args.size == 2) { "Usage: Consumer <partition> <offset>" }
        val partition = TopicPartition("events", args[0].toInt())
        val offset = args[1].toLong()
        val consumerId = System.getenv("CONSUMER_ID")?.toInt() ?: 1

        val record = readRecord(partition, offset)
        saveRecord(record, consumerId)
    }
}

private fun readRecord(partition: TopicPartition, offset: Long): ConsumerRecord<String, String> {
    val bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "kafka1:9092,kafka2:9092,kafka3:9092"
    val config = mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 1,
    )

    KafkaConsumer(config, StringDeserializer(), StringDeserializer()).use { consumer ->
        consumer.assign(listOf(partition))
        consumer.seek(partition, offset)
        val record = consumer.poll(Duration.ofSeconds(10)).firstOrNull()
        check(record != null && record.offset() == offset) { "Record $partition at offset $offset not found" }
        return record
    }
}

private fun saveRecord(record: ConsumerRecord<String, String>, consumerId: Int) {
    val event = consumerMapper.readTree(record.value())
    val eventId = UUID.fromString(event.required("event_id").asString())
    val userId = event.required("user_id").asString()
    val seq = event.required("seq").asLong()
    val url = "jdbc:postgresql://postgres:5432/kafka_lab"
    val password = System.getenv("POSTGRES_PASSWORD") ?: "kafka-lab-local"

    val duplicate = DriverManager.getConnection(url, "kafka_lab", password).use { connection ->
        val sql = """
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
        connection.prepareStatement(sql).use { statement ->
            statement.setObject(1, eventId)
            statement.setString(2, userId)
            statement.setLong(3, seq)
            statement.setInt(4, consumerId)
            statement.setString(5, record.topic())
            statement.setInt(6, record.partition())
            statement.setLong(7, record.offset())
            statement.executeUpdate() == 1
        }
    }

    val action = if (duplicate) "Discarded duplicate" else "Saved"
    println("$action event_id=$eventId topic=${record.topic()} partition=${record.partition()} offset=${record.offset()}")
}
