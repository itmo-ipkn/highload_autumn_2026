package io.github.dmuscipula.kafkalab.consumer

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

private const val DEAD_LETTER_TOPIC = "events.dlt"

internal fun publishDeadLetter(
    producer: KafkaProducer<String, String>,
    record: ConsumerRecord<String, String>,
    reason: String,
) {
    val deadLetter = ProducerRecord(
        DEAD_LETTER_TOPIC,
        record.partition(),
        null,
        record.key(),
        record.value(),
        record.headers(),
    )
    val headers = deadLetter.headers()
    headers.add("error", reason.encodeToByteArray())
    headers.add("original-topic", record.topic().encodeToByteArray())
    headers.add("original-partition", record.partition().toString().encodeToByteArray())
    headers.add("original-offset", record.offset().toString().encodeToByteArray())

    producer.send(deadLetter).get()
    println("Published to $DEAD_LETTER_TOPIC source=${record.topic()}-${record.partition()} offset=${record.offset()}")
}
