package io.github.dmuscipula.kafkalab.consumer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import tools.jackson.databind.json.JsonMapper
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch

private val consumerMapper = JsonMapper.builder().build()

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
        createConsumer(bootstrapServers, consumerId).use { consumer ->
            consumeEvents(consumer = consumer, consumerId = consumerId, stopRequested = stopRequested)
        }
    } finally {
        consumerStopped.countDown()
    }
}

private fun consumeEvents(
    consumer: KafkaConsumer<String, String>,
    consumerId: Int,
    stopRequested: CountDownLatch,
) {
    consumer.subscribe(listOf("events"))
    while (stopRequested.count != 0L) {
        for (record in consumer.poll(Duration.ofSeconds(1))) {
            val event = readEvent(record.value())
            saveRecord(record = record, event = event, consumerId = consumerId)

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

private fun readEvent(value: String): EventToSave {
    val event = consumerMapper.readTree(value)
    val eventId = UUID.fromString(event.required("event_id").asString())
    check(!event.path("payload").path("poison").asBoolean(false)) { "Poison event $eventId requires DLT handling" }

    return EventToSave(
        eventId = eventId,
        userId = event.required("user_id").asString(),
        seq = event.required("seq").asLong(),
    )
}
