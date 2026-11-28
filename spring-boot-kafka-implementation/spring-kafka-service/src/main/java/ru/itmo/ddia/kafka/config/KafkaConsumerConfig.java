package ru.itmo.ddia.kafka.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;
import ru.itmo.ddia.kafka.model.EventMessage;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Configuration
@ConditionalOnProperty(name = "app.role", havingValue = "consumer")
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.pod-id:consumer-local}")
    private String podId;

    @Value("${app.topic.dlt:events.dlt}")
    private String dltTopic;

    /**
     * Tracks the latest processed offset (offset + 1) per TopicPartition so that
     * when a rebalance occurs, onPartitionsRevokedBeforeCommit synchronously commits
     * any already-processed offsets before handing partitions over.
     */
    private final ConcurrentHashMap<TopicPartition, OffsetAndMetadata> processedOffsets = new ConcurrentHashMap<>();

    public ConcurrentHashMap<TopicPartition, OffsetAndMetadata> getProcessedOffsets() {
        return processedOffsets;
    }

    @Bean
    public ConsumerFactory<String, EventMessage> consumerFactory() {
        JsonDeserializer<EventMessage> deserializer = new JsonDeserializer<>(EventMessage.class);
        deserializer.addTrustedPackages("*");
        deserializer.setUseTypeMapperForKey(false);

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "events-processor");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, podId);
        // Disable auto-commit: offsets are committed only after idempotent write to sink
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // Conscious choice of 'earliest': ensures no messages produced before initial group
        // creation are missed; subsequent starts always use committed offsets.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, EventMessage> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> new TopicPartition(dltTopic, record.partition())
        );
        // Spring Kafka itself adds the standard DLT headers (kafka_dlt-original-topic,
        // kafka_dlt-original-partition, kafka_dlt-original-offset, kafka_dlt-exception-*, etc.).
        // Custom headers are attached via addHeadersFunction().
        recoverer.addHeadersFunction((record, exception) -> {
            String reason = exception.getCause() != null
                    ? exception.getCause().getMessage()
                    : exception.getMessage();
            Headers headers = new RecordHeaders();
            headers.add("x-error-reason", (reason != null ? reason : "PoisonEventException")
                    .getBytes(StandardCharsets.UTF_8));
            headers.add("x-original-topic", record.topic().getBytes(StandardCharsets.UTF_8));
            headers.add("x-original-partition", String.valueOf(record.partition()).getBytes(StandardCharsets.UTF_8));
            headers.add("x-original-offset", String.valueOf(record.offset()).getBytes(StandardCharsets.UTF_8));
            headers.add("x-processor-pod", podId.getBytes(StandardCharsets.UTF_8));

            log.warn("[{}] Poison event routed to {} after 3 failed attempts: topic={}, partition={}, offset={}, reason={}",
                    podId, dltTopic, record.topic(), record.partition(), record.offset(), reason);
            return headers;
        });

        // FixedBackOff(150ms interval, 2 retries) = 1 initial try + 2 retries = 3 total attempts
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(150L, 2L));
        // Commit offset after publishing to events.dlt so the partition is not blocked
        handler.setCommitRecovered(true);
        handler.setAckAfterHandle(true);
        return handler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, EventMessage> kafkaListenerContainerFactory(
            ConsumerFactory<String, EventMessage> consumerFactory,
            DefaultErrorHandler errorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, EventMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);

        ContainerProperties containerProps = factory.getContainerProperties();
        containerProps.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        containerProps.setSyncCommits(true);

        // Commit already-processed offsets upon partition rebalance before revoking partitions
        containerProps.setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {
            @Override
            public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                if (partitions.isEmpty()) {
                    return;
                }
                Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
                for (TopicPartition tp : partitions) {
                    OffsetAndMetadata meta = processedOffsets.remove(tp);
                    if (meta != null) {
                        offsetsToCommit.put(tp, meta);
                    }
                }
                if (!offsetsToCommit.isEmpty()) {
                    log.info("[{}] Rebalance triggered: committing processed offsets before partition revocation: {}",
                            podId, offsetsToCommit);
                    consumer.commitSync(offsetsToCommit);
                } else {
                    log.info("[{}] Rebalance triggered: revoking partitions {} (all processed offsets already committed)",
                            podId, partitions);
                }
            }

            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                log.info("[{}] Rebalance completed: assigned partitions {}", podId, partitions);
            }
        });

        return factory;
    }
}
