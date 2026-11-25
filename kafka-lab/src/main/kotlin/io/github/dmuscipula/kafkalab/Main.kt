package io.github.dmuscipula.kafkalab

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import tools.jackson.databind.PropertyNamingStrategies
import tools.jackson.databind.json.JsonMapper
import java.time.Instant
import java.util.UUID

private data class Event(
    val eventId: String,
    val userId: String,
    val type: String,
    val seq: Long,
    val producedAt: String,
    val payload: Map<String, String>,
)

fun main() {
    val bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "127.0.0.1:19092"
    val config = mapOf(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
        ProducerConfig.ACKS_CONFIG to "all",
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
    )
    val event = Event(
        eventId = UUID.randomUUID().toString(),
        userId = "demo-${UUID.randomUUID()}",
        type = "page_view",
        seq = 1,
        producedAt = Instant.now().toString(),
        payload = mapOf("path" to "/"),
    )
    val mapper = JsonMapper.builder().propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE).build()
    val record = ProducerRecord("events", event.userId, mapper.writeValueAsString(event))

    KafkaProducer(config, StringSerializer(), StringSerializer()).use { producer ->
        val metadata = producer.send(record).get()
        println("Sent event_id=${event.eventId} topic=${metadata.topic()} partition=${metadata.partition()} offset=${metadata.offset()}")
    }
}
