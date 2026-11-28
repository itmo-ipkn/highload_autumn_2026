package ru.itmo.ddia.kafka.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record EventMessage(
        @JsonProperty("event_id") UUID eventId,
        @JsonProperty("user_id") String userId,
        @JsonProperty("type") String type,
        @JsonProperty("seq") long seq,
        @JsonProperty("produced_at") Instant producedAt,
        @JsonProperty("payload") Map<String, Object> payload
) {
    public boolean isPoison() {
        return payload != null && Boolean.TRUE.equals(payload.get("poison"));
    }
}
