package io.github.dmuscipula.kafkalab

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import tools.jackson.databind.PropertyNamingStrategies
import tools.jackson.databind.json.JsonMapper
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

private const val USERS_PER_PRODUCER = 3
private const val POISON_EVERY_N_EVENTS = 50

private val eventMapper = JsonMapper.builder()
    .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
    .build()

private data class Event(
    val eventId: String,
    val userId: String,
    val type: String,
    val seq: Long,
    val producedAt: String,
    val payload: Map<String, Any>,
)

fun main() {
    val bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "127.0.0.1:19092"
    val eventIntervalMs = System.getenv("EVENT_INTERVAL_MS")?.toLong() ?: 1_000L
    require(eventIntervalMs > 0) { "EVENT_INTERVAL_MS must be positive" }

    val useUserIdAsKey = when (val keyMode = System.getenv("KEY_MODE") ?: "user_id") {
        "user_id" -> true
        "none" -> false
        else -> error("Unsupported KEY_MODE '$keyMode'. Expected user_id or none.")
    }

    runProducer(
        bootstrapServers = bootstrapServers,
        eventIntervalMs = eventIntervalMs,
        useUserIdAsKey = useUserIdAsKey,
    )
}

private fun runProducer(bootstrapServers: String, eventIntervalMs: Long, useUserIdAsKey: Boolean) {
    val runId = UUID.randomUUID()
    val userIds = List(USERS_PER_PRODUCER) { index -> "demo-$runId-${index + 1}" }

    val stopRequested = CountDownLatch(1)
    val producerStopped = CountDownLatch(1)
    val shutdownHook = Thread({
        println("Producer shutdown requested")
        stopRequested.countDown()
        producerStopped.await()
    }, "producer-shutdown")
    Runtime.getRuntime().addShutdownHook(shutdownHook)

    try {
        createProducer(bootstrapServers).use { producer ->
            var seq = 1L
            publish@ while (true) {
                for ((userIndex, userId) in userIds.withIndex()) {
                    if (stopRequested.count == 0L) break@publish

                    val eventNumber = (seq - 1) * userIds.size + userIndex + 1
                    val event = createEvent(userId = userId, seq = seq, eventNumber = eventNumber)
                    val key = if (useUserIdAsKey) userId else null
                    val record = ProducerRecord<String, String>("events", key, eventMapper.writeValueAsString(event))
                    val metadata = sendEvent(producer = producer, record = record, event = event)
                    println(
                        "Sent event_id=${event.eventId} topic=${metadata.topic()} " +
                            "partition=${metadata.partition()} offset=${metadata.offset()} seq=$seq user_id=$userId",
                    )

                    if (stopRequested.await(eventIntervalMs, TimeUnit.MILLISECONDS)) break@publish
                }
                seq++
            }
        }
        println("Producer stopped")
    } finally {
        producerStopped.countDown()
    }
}

private fun sendEvent(
    producer: KafkaProducer<String, String>,
    record: ProducerRecord<String, String>,
    event: Event,
): RecordMetadata {
    try {
        return producer.send(record).get()
    } catch (exception: Exception) {
        val cause = exception.cause ?: exception
        System.err.println(
            "Failed to send event_id=${event.eventId} user_id=${event.userId} seq=${event.seq}: $cause",
        )
        throw exception
    }
}

private fun createProducer(bootstrapServers: String): KafkaProducer<String, String> {
    val config = mapOf(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
        ProducerConfig.ACKS_CONFIG to "all",
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
    )

    return KafkaProducer(config, StringSerializer(), StringSerializer())
}

private fun createEvent(userId: String, seq: Long, eventNumber: Long): Event {
    val poison = eventNumber % POISON_EVERY_N_EVENTS == 0L

    return Event(
        eventId = UUID.randomUUID().toString(),
        userId = userId,
        type = "page_view",
        seq = seq,
        producedAt = Instant.now().toString(),
        payload = mapOf("path" to "/", "poison" to poison),
    )
}
