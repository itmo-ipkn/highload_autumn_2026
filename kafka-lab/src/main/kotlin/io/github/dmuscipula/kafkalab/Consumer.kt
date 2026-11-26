package io.github.dmuscipula.kafkalab

import io.github.dmuscipula.kafkalab.consumer.runConsumer

object Consumer {
    @JvmStatic
    fun main(args: Array<String>) {
        require(args.isEmpty()) { "Usage: Consumer" }
        val consumerId = System.getenv("CONSUMER_ID")?.toInt() ?: 1
        runConsumer(consumerId)
    }
}
