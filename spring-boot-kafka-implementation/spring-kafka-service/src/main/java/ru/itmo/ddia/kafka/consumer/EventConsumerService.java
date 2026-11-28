package ru.itmo.ddia.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import ru.itmo.ddia.kafka.config.KafkaConsumerConfig;
import ru.itmo.ddia.kafka.model.EventMessage;
import ru.itmo.ddia.kafka.model.PoisonEventException;
import ru.itmo.ddia.kafka.sink.PostgresSinkRepository;

@Service
@ConditionalOnProperty(name = "app.role", havingValue = "consumer")
public class EventConsumerService {

    private static final Logger log = LoggerFactory.getLogger(EventConsumerService.class);

    private final PostgresSinkRepository sinkRepository;
    private final KafkaConsumerConfig consumerConfig;

    @Value("${app.pod-id:consumer-1}")
    private String podId;

    @Value("${app.processing-delay-ms:80}")
    private long processingDelayMs;

    public EventConsumerService(
            PostgresSinkRepository sinkRepository,
            KafkaConsumerConfig consumerConfig
    ) {
        this.sinkRepository = sinkRepository;
        this.consumerConfig = consumerConfig;
    }

    @KafkaListener(
            topics = "${app.topic.events:events}",
            groupId = "events-processor",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void processEvent(
            ConsumerRecord<String, EventMessage> record,
            Acknowledgment acknowledgment
    ) throws InterruptedException {
        EventMessage event = record.value();
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());

        // Emulate processing work
        if (processingDelayMs > 0) {
            Thread.sleep(processingDelayMs);
        }

        // Poison pill check (~2% of events): consumer always fails with an exception.
        // After 3 failed attempts, DefaultErrorHandler publishes to events.dlt and commits offset.
        if (event.isPoison()) {
            log.error("[{}] Encountered POISON event: event_id={}, user_id={}, seq={} at {}[p={}@offset={}]",
                    podId, event.eventId(), event.userId(), event.seq(),
                    record.topic(), record.partition(), record.offset());
            throw new PoisonEventException(
                    "Poison event detected (corrupted_signature=0xDEAD_POISON_PILL) for event_id=" + event.eventId()
            );
        }

        // Idempotent write to shared PostgreSQL sink (deduplication by event_id)
        boolean isNew = sinkRepository.saveIdempotent(
                event,
                podId,
                record.topic(),
                record.partition(),
                record.offset()
        );

        // Record processed offset for rebalance listener safety before committing
        consumerConfig.getProcessedOffsets().put(tp, new OffsetAndMetadata(record.offset() + 1));

        // Explicitly commit offset ONLY after sink persistence completes
        acknowledgment.acknowledge();
        consumerConfig.getProcessedOffsets().remove(tp);

        log.info("[{}] Processed & committed event: event_id={}, user_id={}, seq={}, status={}, {}[p={}@offset={}]",
                podId, event.eventId(), event.userId(), event.seq(),
                isNew ? "INSERTED" : "DEDUP_SKIPPED",
                record.topic(), record.partition(), record.offset());
    }
}
