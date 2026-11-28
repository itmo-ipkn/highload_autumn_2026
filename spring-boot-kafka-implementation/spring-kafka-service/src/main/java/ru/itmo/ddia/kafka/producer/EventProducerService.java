package ru.itmo.ddia.kafka.producer;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.itmo.ddia.kafka.model.EventMessage;
import ru.itmo.ddia.kafka.model.EventType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Service
@ConditionalOnProperty(name = "app.role", havingValue = "producer")
public class EventProducerService {

    private static final Logger log = LoggerFactory.getLogger(EventProducerService.class);
    private static final EventType[] EVENT_TYPES = EventType.values();

    private final KafkaTemplate<String, EventMessage> kafkaTemplate;

    @Value("${app.pod-id:producer-1}")
    private String podId;

    @Value("${app.pod-index:1}")
    private int podIndex;

    @Value("${app.users-per-pod:4}")
    private int usersPerPod;

    @Value("${app.key-mode:user_id}")
    private String keyMode;

    @Value("${app.poison-probability:0.02}")
    private double poisonProbability;

    @Value("${app.topic.events:events}")
    private String eventsTopic;

    private final List<String> ownedUsers = new ArrayList<>();
    private final ConcurrentHashMap<String, AtomicLong> userSeqCounters = new ConcurrentHashMap<>();
    private final AtomicBoolean acceptingNewEvents = new AtomicBoolean(true);

    public EventProducerService(KafkaTemplate<String, EventMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostConstruct
    public void init() {
        int baseUserNumber = podIndex * 100;
        for (int i = 1; i <= usersPerPod; i++) {
            String userId = "user-" + (baseUserNumber + i);
            ownedUsers.add(userId);
            userSeqCounters.put(userId, new AtomicLong(0L));
        }
        log.info("[{}] Initialized Producer Pod (KEY_MODE={}, poisonProbability={}). Exclusively owns users: {}",
                podId, keyMode, poisonProbability, ownedUsers);
    }

    @Scheduled(fixedDelayString = "${app.producer-rate-ms:250}")
    public void generateAndPublishEvent() {
        if (!acceptingNewEvents.get()) {
            return;
        }

        ThreadLocalRandom random = ThreadLocalRandom.current();
        String userId = ownedUsers.get(random.nextInt(ownedUsers.size()));
        long seq = userSeqCounters.get(userId).incrementAndGet();
        EventType eventType = EVENT_TYPES[random.nextInt(EVENT_TYPES.length)];
        boolean isPoison = random.nextDouble() < poisonProbability;

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("producer_pod", podId);
        payload.put("key_mode", keyMode);
        payload.put("poison", isPoison);
        payload.put("session_id", "sess-" + userId + "-2026");
        payload.put("amount", random.nextInt(10, 5000));
        if (isPoison) {
            payload.put("corrupted_signature", "0xDEAD_POISON_PILL");
        }

        EventMessage event = new EventMessage(
                UUID.randomUUID(),
                userId,
                eventType.getValue(),
                seq,
                Instant.now(),
                payload
        );

        // KEY_MODE=user_id uses userId as Kafka record key; KEY_MODE=none sends null key
        String recordKey = "none".equalsIgnoreCase(keyMode) ? null : userId;

        CompletableFuture<SendResult<String, EventMessage>> future =
                kafkaTemplate.send(eventsTopic, recordKey, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[{}] FAILED to publish event: event_id={}, user_id={}, seq={}, key_mode={}, cause={}",
                        podId, event.eventId(), event.userId(), event.seq(), keyMode, ex.getMessage(), ex);
            } else {
                RecordMetadata meta = result.getRecordMetadata();
                log.info("[{}] Sent event: event_id={}, user_id={}, seq={}, type={}, poison={}, key={} -> {}[p={}@offset={}]",
                        podId, event.eventId(), event.userId(), event.seq(), event.type(), isPoison,
                        recordKey, meta.topic(), meta.partition(), meta.offset());
            }
        });
    }

    @PreDestroy
    public void onShutdown() {
        log.info("[{}] SIGTERM received: stopping event generation and flushing buffered Kafka messages...", podId);
        acceptingNewEvents.set(false);
        try {
            kafkaTemplate.flush();
            log.info("[{}] All buffered messages successfully flushed to Kafka cluster.", podId);
        } catch (Exception e) {
            log.error("[{}] Error while flushing Kafka producer buffer during shutdown: {}", podId, e.getMessage(), e);
        }
    }
}
