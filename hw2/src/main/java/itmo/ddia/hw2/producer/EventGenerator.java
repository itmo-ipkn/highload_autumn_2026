package itmo.ddia.hw2.producer;

import itmo.ddia.hw2.model.Event;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public final class EventGenerator {

    private static final List<String> EVENT_TYPES = List.of("page_view", "add_to_cart", "purchase");

    private final String producerId;
    private final int usersPerProducer;
    private final double poisonRate;
    private final Map<String, Long> nextSequence = new HashMap<>();
    private int userIndex;
    private Event pendingEvent;

    public EventGenerator(String producerId, int usersPerProducer, double poisonRate) {
        this.producerId = producerId;
        this.usersPerProducer = usersPerProducer;
        this.poisonRate = poisonRate;
    }

    public Event nextEvent() {
        if (pendingEvent == null) {
            String userId = producerId + "-user-" + (userIndex + 1);
            long sequence = nextSequence.getOrDefault(userId, 1L);
            boolean poison = ThreadLocalRandom.current().nextDouble() < poisonRate;
            Map<String, Object> payload = Map.of(
                    "producer_id", producerId,
                    "poison", poison
            );

            pendingEvent = new Event(
                    UUID.randomUUID().toString(),
                    userId,
                    EVENT_TYPES.get(ThreadLocalRandom.current().nextInt(EVENT_TYPES.size())),
                    sequence,
                    Instant.now().toString(),
                    payload
            );
        }

        return pendingEvent;
    }

    public void acknowledge() {
        if (pendingEvent == null) {
            throw new IllegalStateException("There is no pending event to acknowledge");
        }

        nextSequence.put(pendingEvent.user_id(), pendingEvent.seq() + 1);
        userIndex = (userIndex + 1) % usersPerProducer;
        pendingEvent = null;
    }
}
