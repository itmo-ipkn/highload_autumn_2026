package itmo.ddia.hw2.consumer;

import itmo.ddia.hw2.model.Event;
import itmo.ddia.hw2.persistence.SinkRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
@ConditionalOnProperty(name = "app.role", havingValue = "consumer")
public class EventProcessor {

    private static final int MAX_ATTEMPTS = 3;

    private final long processingDelayMs;
    private final String podId;
    private final ObjectMapper objectMapper;
    private final SinkRepository sinkRepository;
    private final DeadLetterPublisher deadLetterPublisher;

    public EventProcessor(
            @Value("${consumer.processing-delay-ms}") long processingDelayMs,
            @Value("${consumer.pod-id}") String podId,
            ObjectMapper objectMapper,
            SinkRepository sinkRepository,
            DeadLetterPublisher deadLetterPublisher
    ) {
        if (processingDelayMs < 0) {
            throw new IllegalArgumentException("PROCESSING_DELAY_MS must not be negative");
        }
        this.processingDelayMs = processingDelayMs;
        this.podId = podId;
        this.objectMapper = objectMapper;
        this.sinkRepository = sinkRepository;
        this.deadLetterPublisher = deadLetterPublisher;
    }

    public void process(ConsumerRecord<String, byte[]> record)
            throws InterruptedException, ExecutionException {
        Exception lastFailure = null;
        Event event = null;

        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                if (processingDelayMs > 0) {
                    TimeUnit.MILLISECONDS.sleep(processingDelayMs);
                }

                event = objectMapper.readValue(record.value(), Event.class);
                if (isPoison(event)) {
                    throw new IllegalStateException("Poison event requested to fail during processing");
                }

                boolean inserted = sinkRepository.saveEvent(
                        event, podId, record.topic(), record.partition(), record.offset()
                );

                if (!inserted) {
                    log.info("Dropped duplicate event_id={} from {}-{}@{}",
                            event.event_id(), record.topic(), record.partition(), record.offset());
                }

                log.debug("Processed event_id={} from {}-{}@{}",
                        event.event_id(), record.topic(), record.partition(), record.offset());

                return;
            } catch (InterruptedException exception) {
                throw exception;
            } catch (Exception exception) {
                lastFailure = exception;

                log.warn("Processing attempt {}/{} failed for {}-{}@{}",
                        attempt, MAX_ATTEMPTS, record.topic(), record.partition(), record.offset(), exception);
            }
        }

        deadLetterPublisher.publish(record, lastFailure);
    }

    private boolean isPoison(Event event) {
        return event.payload() != null && Boolean.TRUE.equals(event.payload().get("poison"));
    }
}
