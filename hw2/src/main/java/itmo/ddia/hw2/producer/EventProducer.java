package itmo.ddia.hw2.producer;

import itmo.ddia.hw2.model.Event;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
@ConditionalOnProperty(name = "app.role", havingValue = "producer")
public class EventProducer {

    private final String producerId;
    private final String topic;
    private final int eventsPerSecond;
    private final String keyMode;
    private final String bootstrapServers;
    private final ObjectMapper objectMapper;
    private final EventGenerator eventGenerator;

    private KafkaProducer<String, byte[]> producer;
    private ScheduledExecutorService scheduler;

    public EventProducer(
            @Value("${producer.id}") String producerId,
            @Value("${producer.topic}") String topic,
            @Value("${producer.users-per-producer}") int usersPerProducer,
            @Value("${producer.events-per-second}") int eventsPerSecond,
            @Value("${producer.key-mode}") String keyMode,
            @Value("${producer.poison-rate}") double poisonRate,
            @Value("${kafka.bootstrap-servers}") String bootstrapServers,
            ObjectMapper objectMapper
    ) {
        this.producerId = producerId;
        this.topic = topic;
        this.eventsPerSecond = eventsPerSecond;
        this.keyMode = keyMode;
        this.bootstrapServers = bootstrapServers;
        this.objectMapper = objectMapper;
        validateSettings(usersPerProducer, eventsPerSecond, keyMode, poisonRate);
        this.eventGenerator = new EventGenerator(producerId, usersPerProducer, poisonRate);
    }

    @PostConstruct
    void start() {
        producer = new KafkaProducer<>(producerProperties());
        scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "event-producer-" + producerId);
            thread.setDaemon(false);
            return thread;
        });

        long intervalNanos = TimeUnit.SECONDS.toNanos(1) / eventsPerSecond;
        scheduler.scheduleWithFixedDelay(this::sendNextEvent, 0, intervalNanos, TimeUnit.NANOSECONDS);

        log.info("Started producer {}: topic={}, rate={} events/s, keyMode={}",
                producerId, topic, eventsPerSecond, keyMode);
    }

    private void sendNextEvent() {
        Event event = eventGenerator.nextEvent();
        try {
            byte[] value = objectMapper.writeValueAsBytes(event);
            String key = keyMode.equals("user_id") ? event.user_id() : null;
            producer.send(new ProducerRecord<>(topic, key, value)).get();
            eventGenerator.acknowledge();

            log.debug("Produced event {} for {} seq={}", event.event_id(), event.user_id(), event.seq());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            log.error("Interrupted while sending event {} from {}", event.event_id(), producerId, exception);
        } catch (JacksonException exception) {
            log.error("Failed to serialize event {} from {}; it will be retried",
                    event.event_id(), producerId, exception);
        } catch (ExecutionException | RuntimeException exception) {
            log.error("Failed to send event {} from {}; it will be retried",
                    event.event_id(), producerId, exception);
        }
    }

    private Properties producerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, producerId);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return properties;
    }

    private void validateSettings(int usersPerProducer, int rate, String keyMode, double poisonRate) {
        if (usersPerProducer < 1) {
            throw new IllegalArgumentException("USERS_PER_PRODUCER must be at least 1");
        }

        if (rate < 1 || rate > TimeUnit.SECONDS.toNanos(1)) {
            throw new IllegalArgumentException("EVENTS_PER_SECOND must be between 1 and 1000000000");
        }

        if (!keyMode.equals("user_id") && !keyMode.equals("none")) {
            throw new IllegalArgumentException("KEY_MODE must be user_id or none");
        }

        if (poisonRate < 0 || poisonRate > 1) {
            throw new IllegalArgumentException("POISON_RATE must be between 0 and 1");
        }
    }

    @PreDestroy
    void stop() {
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException exception) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();

                log.warn("Interrupted while stopping producer scheduler {}", producerId, exception);
            }
        }

        if (producer != null) {
            producer.flush();
            producer.close(Duration.ofSeconds(30));

            log.info("Stopped producer {}", producerId);
        }
    }
}
