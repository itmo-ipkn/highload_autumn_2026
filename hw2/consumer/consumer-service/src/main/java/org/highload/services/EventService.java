package org.highload.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.highload.dto.EventDto;
import org.highload.propeties.KafkaProperty;
import org.highload.repositories.ConsumerMetricsRepository;
import org.highload.repositories.EventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventService {
    private final EventRepository eventRepository;
    private final KafkaProperty kafkaProperty;
    private final ConsumerMetricsRepository consumerMetricsRepository;

    @Transactional
    public void save(EventDto eventDto, ConsumerRecord<String, String> record) {
        int podId = kafkaProperty.getPodId();
        // The INSERT locks this user until commit before assigning sink_order.
        int inserted = eventRepository.insertIfAbsent(
                eventDto.getEventId(),
                eventDto.getUserId(),
                eventDto.getSeq(),
                podId,
                record.topic(),
                record.partition(),
                record.offset(),
                eventDto.getType().getValue(),
                eventDto.getProducedAt(),
                eventDto.getPayload() == null ? null : eventDto.getPayload().toString());

        if (inserted == 0) {
            consumerMetricsRepository.incrementDuplicates(podId);
            log.error("Duplicate event: eventId={}, podId={}", eventDto.getEventId(), podId);
        }
    }

}
