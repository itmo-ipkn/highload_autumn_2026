package org.producer.services;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.producer.dto.EventDto;
import org.producer.properties.KafkaProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Map<Long, Long> seqByUsers = new HashMap<>();
    private final ObjectMapper objectMapper;
    private final KafkaProperty kafkaProperty;
    private final EventFactory eventFactory;
    private final EventSequenceRecovery sequenceRecovery;
    private ScheduledFuture<?> publishingTask;
    private EventDto pendingEvent;

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        seqByUsers.putAll(sequenceRecovery.load());
        long period = TimeUnit.SECONDS.toNanos(1) / kafkaProperty.getRate();
        publishingTask = scheduledExecutorService.scheduleAtFixedRate(this::sendMessage, 0, period, TimeUnit.NANOSECONDS);
        log.info("Event publisher started: rate={}, keyMode={}", kafkaProperty.getRate(), kafkaProperty.getKeyMode());
    }

    private void sendMessage() {
        try {
            if (pendingEvent == null) {
                pendingEvent = eventFactory.generate();
                long userId = pendingEvent.getUserId();
                pendingEvent.setSeq(seqByUsers.getOrDefault(userId, 0L) + 1);
            }

            Long userId = pendingEvent.getUserId();
            String key = "user_id".equals(kafkaProperty.getKeyMode()) ? userId.toString() : null;
            String payload = objectMapper.writeValueAsString(pendingEvent);
            kafkaTemplate.send(kafkaProperty.getTopic(), key, payload).join();
            seqByUsers.put(userId, pendingEvent.getSeq());
            pendingEvent = null;
        } catch (Exception e) {
            log.error("Failed to generate or publish event; a pending event will be retried", e);
        }
    }

    @PreDestroy
    public void stop() {
        log.info("Stopping event publisher");
        if (publishingTask != null) {
            publishingTask.cancel(false);
        }

        scheduledExecutorService.shutdown();

        try {
            if (!scheduledExecutorService.awaitTermination(150, TimeUnit.SECONDS)) {
                scheduledExecutorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduledExecutorService.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            kafkaTemplate.flush();
        }

        log.info("Event publisher stopped");
    }
}
