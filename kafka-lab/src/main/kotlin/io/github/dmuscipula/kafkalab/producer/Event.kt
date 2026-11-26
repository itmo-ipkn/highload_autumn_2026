package io.github.dmuscipula.kafkalab.producer

import java.time.Instant
import java.util.UUID

private const val POISON_EVERY_N_EVENTS = 50

internal data class Event(
    val eventId: String,
    val userId: String,
    val type: String,
    val seq: Long,
    val producedAt: String,
    val payload: Map<String, Any>,
)

internal fun createEvent(userId: String, seq: Long, eventNumber: Long): Event {
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
