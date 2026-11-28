package itmo.ddia.hw2.model;

import java.util.Map;

public record Event(
        String event_id,
        String user_id,
        String type,
        long seq,
        String produced_at,
        Map<String, Object> payload
) {
}
