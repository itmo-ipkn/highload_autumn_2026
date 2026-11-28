package org.producer.services;

import lombok.RequiredArgsConstructor;
import org.producer.dto.EventDto;
import org.producer.enums.EventType;
import org.producer.properties.UserProperty;
import org.springframework.stereotype.Service;

import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Service
@RequiredArgsConstructor
public class EventFactory {

    private static final EventType[] EVENT_TYPES = EventType.values();
    private final UserProperty userProperty;
    private final ObjectMapper objectMapper;

    public EventDto generate() {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        return EventDto.builder()
                .eventId(UUID.randomUUID())
                .userId(random.nextLong(
                        userProperty.getMinId(),
                        userProperty.getMaxId() + 1
                ))
                .type(EVENT_TYPES[random.nextInt(EVENT_TYPES.length)])
                .producedAt(Instant.now())
                .payload(objectMapper.createObjectNode().put("poison", random.nextInt(100) < 2))
                .build();
    }
}
