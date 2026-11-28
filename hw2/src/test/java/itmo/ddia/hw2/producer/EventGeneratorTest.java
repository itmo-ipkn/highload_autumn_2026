package itmo.ddia.hw2.producer;

import itmo.ddia.hw2.model.Event;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventGeneratorTest {
    @Test
    void retriesPendingEventUntilKafkaAcknowledgesIt() {
        EventGenerator generator = new EventGenerator("producer-1", 2, 0);

        Event first = generator.nextEvent();
        assertSame(first, generator.nextEvent());
        assertEquals(1, first.seq());

        generator.acknowledge();
        Event secondUser = generator.nextEvent();
        assertEquals("producer-1-user-2", secondUser.user_id());
        assertEquals(1, secondUser.seq());

        generator.acknowledge();
        Event nextSequence = generator.nextEvent();
        assertEquals("producer-1-user-1", nextSequence.user_id());
        assertEquals(2, nextSequence.seq());
    }

    @Test
    void marksPoisonEventsAccordingToConfiguredRate() {
        EventGenerator generator = new EventGenerator("producer-1", 1, 1);

        assertTrue(Boolean.TRUE.equals(generator.nextEvent().payload().get("poison")));
    }
}
