package io.github.dmuscipula.highloadlab.service

import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.server.ResponseStatusException
import kotlin.random.Random
import kotlin.random.nextLong

@Service
class RequestSimulator(
    @Value($$"${simulation.error-probability}") private val errorProbability: Double,
    @Value($$"${simulation.min-delay-ms}") private val minDelayMs: Long,
    @Value($$"${simulation.max-delay-ms}") private val maxDelayMs: Long,
) {

    private val errorStatuses = listOf(HttpStatus.INTERNAL_SERVER_ERROR, HttpStatus.SERVICE_UNAVAILABLE)

    init {
        require(errorProbability in 0.0..1.0) { "Error probability must be between 0 and 1" }
        require(minDelayMs in 0..maxDelayMs) { "Expected 0 <= min delay <= max delay" }
    }

    fun simulateWork() {
        val delayMs = Random.nextLong(minDelayMs..maxDelayMs)
        Thread.sleep(delayMs)

        if (Random.nextDouble() < errorProbability) {
            throw ResponseStatusException(errorStatuses.random(), "Simulated error")
        }
    }
}
