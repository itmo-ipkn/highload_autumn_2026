package org.highload.listeners;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.highload.dto.EventDto;
import org.highload.propeties.KafkaProperty;
import org.highload.services.EventService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventListener {
    private final EventService eventService;
    private final ObjectMapper objectMapper;
    private final KafkaProperty kafkaProperty;

    @KafkaListener(topics = "${app.kafka.topic}")
    public void consume(ConsumerRecord<String, String> record) throws InterruptedException {
        var json = objectMapper.readTree(record.value());
        // Earlier producers encoded the JSON object as a JSON string.
        if (json != null && json.isTextual()) {
            json = objectMapper.readTree(json.asString());
        }
        EventDto event = objectMapper.treeToValue(json, EventDto.class);
        validate(event);
        Thread.sleep(kafkaProperty.getProcessingDelayMs());

        if (event.getPayload() != null && event.getPayload().path("poison").asBoolean(false)) {
            throw new PoisonEventException("Poison event " + event.getEventId());
        }
        eventService.save(event, record);
    }

    private void validate(EventDto event) {
        if (event == null
                || event.getEventId() == null
                || event.getUserId() == null
                || event.getType() == null
                || event.getProducedAt() == null
                || event.getSeq() == null
                || event.getSeq() <= 0) {

            throw new IllegalArgumentException(
                    "Invalid event payload"
            );
        }
    }

}
