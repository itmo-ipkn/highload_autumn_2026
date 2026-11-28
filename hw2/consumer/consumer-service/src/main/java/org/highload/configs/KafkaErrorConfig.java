package org.highload.configs;

import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.highload.listeners.PoisonEventException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorConfig {
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        var recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate, (record, exception) -> new TopicPartition("events.dlt", record.partition()));
        recoverer.setFailIfSendResultIsError(true);

        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000, FixedBackOff.UNLIMITED_ATTEMPTS));
        handler.setBackOffFunction((record, exception) -> {
            for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
                if (cause instanceof PoisonEventException) {
                    return new FixedBackOff(0, 2);
                }
            }
            return null;
        });
        handler.setClassifications(Map.of(Exception.class, true), true);
        return handler;
    }
}
