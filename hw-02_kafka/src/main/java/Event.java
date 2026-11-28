import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

public record Event(
        @JsonProperty("event_id")    UUID eventId,
        @JsonProperty("user_id")     UUID userId,
        @JsonProperty("type")        EventType type,
        @JsonProperty("seq")         long seq,
        @JsonProperty("produced_at") Instant producedAt,
        @JsonProperty("payload") JsonNode payload
) {
    @JsonIgnore
    public boolean isPoison() {
        return payload != null && payload.path("poison").asBoolean(false);
    }
}
