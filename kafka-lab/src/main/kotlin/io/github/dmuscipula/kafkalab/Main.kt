package io.github.dmuscipula.kafkalab

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import tools.jackson.databind.PropertyNamingStrategies
import tools.jackson.databind.json.JsonMapper
import java.time.Instant
import java.util.UUID

private const val USERS_PER_PRODUCER = 3

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
    val eventIntervalMs = System.getenv("EVENT_INTERVAL_MS")?.toLong() ?: 1_000L
    require(eventIntervalMs > 0) { "EVENT_INTERVAL_MS must be positive" }

    val config = mapOf(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
        ProducerConfig.ACKS_CONFIG to "all",
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
    )
    val mapper = JsonMapper.builder().propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE).build()
    val runId = UUID.randomUUID()
    val userIds = List(USERS_PER_PRODUCER) { index -> "demo-$runId-${index + 1}" }
    var seq = 1L

    KafkaProducer(config, StringSerializer(), StringSerializer()).use { producer ->
        while (true) {
            for (userId in userIds) {
                val event = Event(
                    eventId = UUID.randomUUID().toString(),
                    userId = userId,
                    type = "page_view",
                    seq = seq,
                    producedAt = Instant.now().toString(),
                    payload = mapOf("path" to "/"),
                )
                val record = ProducerRecord("events", userId, mapper.writeValueAsString(event))
                val metadata = producer.send(record).get()
                println(
                    "Sent event_id=${event.eventId} topic=${metadata.topic()} " +
                        "partition=${metadata.partition()} offset=${metadata.offset()} seq=$seq user_id=$userId",
                )

                Thread.sleep(eventIntervalMs)
            }
            seq++
        }
    }
}
