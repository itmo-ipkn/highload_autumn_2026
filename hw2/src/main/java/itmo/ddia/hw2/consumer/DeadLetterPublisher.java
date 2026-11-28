package itmo.ddia.hw2.consumer;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Component
@Slf4j
@ConditionalOnProperty(name = "app.role", havingValue = "consumer")
public class DeadLetterPublisher {

    private static final String ORIGINAL_TOPIC = "original_topic";
    private static final String ORIGINAL_PARTITION = "original_partition";
    private static final String ORIGINAL_OFFSET = "original_offset";
    private static final String ERROR_REASON = "error_reason";

    private final String dltTopic;
    private final KafkaProducer<String, byte[]> producer;

    public DeadLetterPublisher(
            @Value("${consumer.dlt-topic}") String dltTopic,
            @Value("${kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${consumer.pod-id}") String podId
    ) {
        this.dltTopic = dltTopic;
        this.producer = new KafkaProducer<>(producerProperties(bootstrapServers, podId));
    }

    public void publish(ConsumerRecord<String, byte[]> source, Exception failure)
            throws InterruptedException, ExecutionException {
        Headers headers = new RecordHeaders();
        headers.add(ORIGINAL_TOPIC, bytes(source.topic()));
        headers.add(ORIGINAL_PARTITION, bytes(source.partition()));
        headers.add(ORIGINAL_OFFSET, bytes(source.offset()));
        headers.add(ERROR_REASON, bytes(failureReason(failure)));

        ProducerRecord<String, byte[]> deadLetter = new ProducerRecord<>(
                dltTopic, null, source.key(), source.value(), headers
        );
        producer.send(deadLetter).get();
        log.error("Sent {}-{}@{} to {} after processing failure",
                source.topic(), source.partition(), source.offset(), dltTopic, failure);
    }

    private Properties producerProperties(String bootstrapServers, String podId) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, podId + "-dlt");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return properties;
    }

    private byte[] bytes(Object value) {
        return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
    }

    private String failureReason(Exception failure) {
        String message = failure.getMessage();
        return failure.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }

    @PreDestroy
    void close() {
        producer.close(Duration.ofSeconds(10));
    }
}
