package io.github.dmuscipula.kafkalab

import io.github.dmuscipula.kafkalab.producer.runProducer

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
