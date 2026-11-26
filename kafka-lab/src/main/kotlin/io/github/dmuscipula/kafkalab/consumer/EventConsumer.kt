package io.github.dmuscipula.kafkalab.consumer

import io.github.dmuscipula.kafkalab.producer.createProducer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import tools.jackson.databind.json.JsonMapper
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch

private const val MAX_PROCESSING_ATTEMPTS = 3

private val consumerMapper = JsonMapper.builder().build()

private class PoisonEventException(eventId: UUID) : RuntimeException("Poison event $eventId")

internal fun runConsumer(consumerId: Int) {
    val bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "kafka1:9092,kafka2:9092,kafka3:9092"
    val stopRequested = CountDownLatch(1)
    val consumerStopped = CountDownLatch(1)
    val shutdownHook = Thread({
        stopRequested.countDown()
        consumerStopped.await()
    }, "consumer-shutdown")
    Runtime.getRuntime().addShutdownHook(shutdownHook)

    try {
        createProducer(bootstrapServers).use { deadLetterProducer ->
            createConsumer(bootstrapServers, consumerId).use { consumer ->
                consumeEvents(
                    consumer = consumer,
                    deadLetterProducer = deadLetterProducer,
                    consumerId = consumerId,
                    stopRequested = stopRequested,
                )
            }
        }
    } finally {
        consumerStopped.countDown()
    }
}

private fun consumeEvents(
    consumer: KafkaConsumer<String, String>,
    deadLetterProducer: KafkaProducer<String, String>,
    consumerId: Int,
    stopRequested: CountDownLatch,
) {
    consumer.subscribe(listOf("events"))
    while (stopRequested.count != 0L) {
        for (record in consumer.poll(Duration.ofSeconds(1))) {
            processRecord(record = record, consumerId = consumerId, deadLetterProducer = deadLetterProducer)

            val partition = TopicPartition(record.topic(), record.partition())
            val nextOffset = OffsetAndMetadata(record.offset() + 1)
            consumer.commitSync(mapOf(partition to nextOffset))
        }
    }
}

private fun createConsumer(bootstrapServers: String, consumerId: Int): KafkaConsumer<String, String> {
    val config = mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
        ConsumerConfig.GROUP_ID_CONFIG to "events-processor",
        ConsumerConfig.CLIENT_ID_CONFIG to "consumer-$consumerId",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 1,
    )

    return KafkaConsumer(config, StringDeserializer(), StringDeserializer())
}

private fun processRecord(
    record: ConsumerRecord<String, String>,
    consumerId: Int,
    deadLetterProducer: KafkaProducer<String, String>,
) {
    for (attempt in 1..MAX_PROCESSING_ATTEMPTS) {
        try {
            val event = readEvent(record.value())
            saveRecord(record = record, event = event, consumerId = consumerId)
            return
        } catch (exception: PoisonEventException) {
            System.err.println(
                "Processing attempt $attempt/$MAX_PROCESSING_ATTEMPTS failed " +
                    "topic=${record.topic()} partition=${record.partition()} offset=${record.offset()}: ${exception.message}",
            )
            if (attempt == MAX_PROCESSING_ATTEMPTS) {
                publishDeadLetter(
                    producer = deadLetterProducer,
                    record = record,
                    reason = exception.message.orEmpty(),
                )
            }
        }
    }
}

private fun readEvent(value: String): EventToSave {
    val event = consumerMapper.readTree(value)
    val eventId = UUID.fromString(event.required("event_id").asString())
    if (event.path("payload").path("poison").asBoolean(false)) throw PoisonEventException(eventId)

    return EventToSave(
        eventId = eventId,
        userId = event.required("user_id").asString(),
        seq = event.required("seq").asLong(),
    )
}
