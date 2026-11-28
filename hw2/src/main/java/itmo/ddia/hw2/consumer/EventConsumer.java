package itmo.ddia.hw2.consumer;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Slf4j
@ConditionalOnProperty(name = "app.role", havingValue = "consumer")
public class EventConsumer {

    private final String podId;
    private final String groupId;
    private final String topic;
    private final String bootstrapServers;
    private final EventProcessor eventProcessor;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private volatile KafkaConsumer<String, byte[]> consumer;
    private Thread worker;

    public EventConsumer(
            @Value("${consumer.pod-id}") String podId,
            @Value("${consumer.group-id}") String groupId,
            @Value("${consumer.topic}") String topic,
            @Value("${kafka.bootstrap-servers}") String bootstrapServers,
            EventProcessor eventProcessor
    ) {
        this.podId = podId;
        this.groupId = groupId;
        this.topic = topic;
        this.bootstrapServers = bootstrapServers;
        this.eventProcessor = eventProcessor;
    }

    @PostConstruct
    void start() {
        worker = new Thread(this::pollMessages, "event-consumer-" + podId);
        worker.setDaemon(false);
        worker.start();
    }

    private void pollMessages() {
        try (KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(consumerProperties())) {
            consumer = kafkaConsumer;
            kafkaConsumer.subscribe(List.of(topic), rebalanceListener());

            log.info("Started consumer {} in group {}, topic={}, offsetReset=earliest",
                    podId, groupId, topic);

            while (running.get()) {
                for (ConsumerRecord<String, byte[]> record : kafkaConsumer.poll(Duration.ofMillis(500))) {
                    if (!running.get()) {
                        break;
                    }

                    processAndCommit(kafkaConsumer, record);
                }
            }
        } catch (WakeupException exception) {
            if (running.get()) {
                log.error("Consumer {} was woken unexpectedly", podId, exception);
            }
        } catch (Exception exception) {
            if (running.get()) {
                log.error("Consumer {} stopped unexpectedly", podId, exception);
            }
        } finally {
            consumer = null;

            log.info("Consumer {} stopped", podId);
        }
    }

    private void processAndCommit(
            KafkaConsumer<String, byte[]> kafkaConsumer,
            ConsumerRecord<String, byte[]> record
    ) {
        try {
            eventProcessor.process(record);
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            OffsetAndMetadata nextOffset = new OffsetAndMetadata(record.offset() + 1);
            kafkaConsumer.commitSync(Map.of(partition, nextOffset));

            log.debug("Committed {} for {}-{}@{}",
                    nextOffset.offset(), record.topic(), record.partition(), record.offset());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            running.set(false);

            log.info("Consumer {} interrupted while processing {}-{}@{}",
                    podId, record.topic(), record.partition(), record.offset());
        } catch (Exception exception) {
            log.error("Failed to complete {}-{}@{}; seeking back so it can be retried",
                    record.topic(), record.partition(), record.offset(), exception);

            kafkaConsumer.seek(
                    new TopicPartition(record.topic(), record.partition()),
                    record.offset()
            );

            pauseBeforeRetry();
        }
    }

    private ConsumerRebalanceListener rebalanceListener() {
        return new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.info("Consumer {} revoking partitions {}; all completed records were synchronously committed",
                        podId, partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.info("Consumer {} assigned partitions {}", podId, partitions);
            }

            @Override
            public void onPartitionsLost(Collection<TopicPartition> partitions) {
                log.warn("Consumer {} lost partitions {}", podId, partitions);
            }
        };
    }

    private Properties consumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, podId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return properties;
    }

    private void pauseBeforeRetry() {
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            running.set(false);
        }
    }

    @PreDestroy
    void stop() {
        running.set(false);
        KafkaConsumer<String, byte[]> currentConsumer = consumer;
        if (currentConsumer != null) {
            currentConsumer.wakeup();
        }

        if (worker != null) {
            try {
                worker.join(TimeUnit.SECONDS.toMillis(30));

                if (worker.isAlive()) {
                    log.warn("Consumer {} did not stop within 30 seconds", podId);
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();

                log.warn("Interrupted while waiting for consumer {} to stop", podId, exception);
            }
        }
    }
}
