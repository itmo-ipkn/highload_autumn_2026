package io.github.dmuscipula.kafkalab.producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import tools.jackson.databind.PropertyNamingStrategies
import tools.jackson.databind.json.JsonMapper
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

private const val USERS_PER_PRODUCER = 3

private val eventMapper = JsonMapper.builder()
    .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
    .build()

internal fun runProducer(bootstrapServers: String, eventIntervalMs: Long, useUserIdAsKey: Boolean) {
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

internal fun createProducer(bootstrapServers: String): KafkaProducer<String, String> {
    val config = mapOf(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
        ProducerConfig.ACKS_CONFIG to "all",
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
    )

    return KafkaProducer(config, StringSerializer(), StringSerializer())
}
