package tech.soba4ka.hw2.model;

import java.util.Map;
import java.util.UUID;

public record Event(UUID event_id, String user_id, String type, long seq, String produced_at, Map<String,Object> payload) {
    public boolean poison() { return Boolean.TRUE.equals(payload.get("poison")); }
}
