package io.github.dmuscipula.kafkalab

import io.github.dmuscipula.kafkalab.consumer.runConsumer

object Consumer {
    @JvmStatic
    fun main(args: Array<String>) {
        require(args.isEmpty()) { "Usage: Consumer" }
        val consumerId = System.getenv("CONSUMER_ID")?.toInt() ?: 1
        val processingDelayMs = System.getenv("PROCESSING_DELAY_MS")?.toLong() ?: 50L
        require(processingDelayMs >= 0) { "PROCESSING_DELAY_MS must be non-negative" }

        runConsumer(consumerId, processingDelayMs)
    }
}
