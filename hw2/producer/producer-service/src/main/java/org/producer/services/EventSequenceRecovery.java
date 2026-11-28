package org.producer.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.producer.properties.KafkaProperty;
import org.producer.properties.UserProperty;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class EventSequenceRecovery {
    private final KafkaProperty kafkaProperty;
    private final UserProperty userProperty;
    private final ObjectMapper objectMapper;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    Map<Long, Long> load() {
        var settings = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        var sequences = new HashMap<Long, Long>();
        try (var consumer = new KafkaConsumer<>(settings, new StringDeserializer(), new StringDeserializer())) {
            var partitions = consumer.partitionsFor(kafkaProperty.getTopic()).stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition()))
                    .toList();
            consumer.assign(partitions);
            var endOffsets = consumer.endOffsets(partitions);
            consumer.seekToBeginning(partitions);
            while (partitions.stream().anyMatch(partition -> consumer.position(partition) < endOffsets.get(partition))) {
                for (var record : consumer.poll(Duration.ofMillis(500))) {
                    var partition = new TopicPartition(record.topic(), record.partition());
                    if (record.value() != null && record.offset() < endOffsets.get(partition)) {
                        restoreSequence(record.value(), sequences);
                    }
                }
            }
        }
        log.info("Restored sequence numbers for {} users", sequences.size());
        return sequences;
    }

    private void restoreSequence(String value, Map<Long, Long> sequences) {
        JsonNode json;
        try {
            json = objectMapper.readTree(value);
            if (json.isTextual()) {
                json = objectMapper.readTree(json.asString());
            }
        } catch (JacksonException ignored) {
            return;
        }

        var user = json.has("user_id") ? json.path("user_id") : json.path("userId");
        long userId = user.asLong(0);
        long seq = json.path("seq").asLong(0);
        if (userId >= userProperty.getMinId() && userId <= userProperty.getMaxId() && seq > 0) {
            sequences.merge(userId, seq, Math::max);
        }
    }
}
