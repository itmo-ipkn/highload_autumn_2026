package org.producer.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.producer.enums.EventType;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventDto {
    @JsonProperty("event_id")
    @JsonAlias("eventId")
    private UUID eventId;
    @JsonProperty("user_id")
    @JsonAlias("userId")
    private Long userId;
    private EventType type;
    private Long seq;
    @JsonProperty("produced_at")
    @JsonAlias("producedAt")
    private Instant producedAt;
    private JsonNode payload;
}
