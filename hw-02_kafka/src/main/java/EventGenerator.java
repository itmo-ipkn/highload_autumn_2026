import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

public class EventGenerator {

    private final List<UUID> ownedUsers = new ArrayList<>();
    private final Map<UUID, Long> lastSeq = new HashMap<>();
    private final Random random;
    private final double poisonRate;

    public EventGenerator(int podIndex, int podCount, int totalUsers, double poisonRate, long seed) {
        this.poisonRate = poisonRate;
        this.random = new Random(seed);
        for (int n = 0; n < totalUsers; n++) {
            if (n % podCount == podIndex) {
                UUID id = UUID.nameUUIDFromBytes(("user-" + n).getBytes(StandardCharsets.UTF_8));
                ownedUsers.add(id);
                lastSeq.put(id, 0L);
            }
        }
    }

    public Event next() {
        UUID userId = ownedUsers.get(random.nextInt(ownedUsers.size()));
        long seq = lastSeq.merge(userId, 1L, Long::sum);

        EventType type = EventType.values()[random.nextInt(EventType.values().length)];
        ObjectNode payload = Json.MAPPER.createObjectNode();
        switch (type) {
            case PAGE_VIEW   -> payload.put("url", "/product/" + random.nextInt(1000));
            case ADD_TO_CART -> payload.put("sku", "SKU-" + random.nextInt(1000))
                    .put("qty", 1 + random.nextInt(3));
            case PURCHASE    -> payload.put("order_id", UUID.randomUUID().toString())
                    .put("amount", Math.round(random.nextDouble() * 10000) / 100.0);
        }
        if (random.nextDouble() < poisonRate) {
            payload.put("poison", true);
        }

        return new Event(UUID.randomUUID(), userId, type, seq, Instant.now(), payload);
    }
}
