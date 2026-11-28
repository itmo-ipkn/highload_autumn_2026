import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class ConsumerApp {
    private static final Logger log = LoggerFactory.getLogger(ConsumerApp.class);

    private static final class StopRequested extends RuntimeException {
        StopRequested() { super(null, null, false, false); }
    }

    @FunctionalInterface private interface Action<T> { T run() throws Exception; }

    private final int pod;
    private final String topic, dltTopic;
    private final long delayMs;
    private final int maxAttempts;

    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> dltProducer;
    private final Sink sink;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Map<TopicPartition, OffsetAndMetadata> pending = new HashMap<>();

    private final AtomicLong written = new AtomicLong(), duplicates = new AtomicLong(),
            toDlt = new AtomicLong(), infraRetries = new AtomicLong();

    private ConsumerApp() {
        String bootstrap = env("BOOTSTRAP_SERVERS", "kafka-1:19092,kafka-2:19092,kafka-3:19092");
        this.topic       = env("TOPIC", "events");
        this.dltTopic    = env("DLT_TOPIC", "events.dlt");
        this.pod         = Integer.parseInt(env("POD_INDEX", "0"));
        this.delayMs     = Long.parseLong(env("PROCESSING_DELAY_MS", "10"));
        this.maxAttempts = Integer.parseInt(env("MAX_ATTEMPTS", "3"));
        String groupId   = env("GROUP_ID", "events-processor");

        Properties c = new Properties();
        c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        c.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        c.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-" + pod);
        c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        c.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        c.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic");
        c.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
        c.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");
        c.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "2000");
        c.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50");
        c.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000");
        this.consumer = new KafkaConsumer<>(c);

        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "dlt-producer-" + pod);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");
        this.dltProducer = new KafkaProducer<>(p);

        this.sink = new Sink(
                env("DB_URL", "jdbc:postgresql://postgres:5432/sink"),
                env("DB_USER", "sink"), env("DB_PASSWORD", "sink"));

        log.info("START consumer pod={} group={} topic={} dlt={} delay_ms={} max_attempts={} auto_offset_reset=earliest",
                pod, groupId, topic, dltTopic, delayMs, maxAttempts);
    }

    public static void main(String[] args) {
        ConsumerApp app = new ConsumerApp();
        Thread main = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            app.running.set(false);
            app.consumer.wakeup();
            try { main.join(); } catch (InterruptedException ignored) { }
        }, "shutdown-hook"));
        app.run();
    }

    // ------------------------------------------------------------------ основной цикл

    private void run() {
        ScheduledExecutorService stats = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "stats"); t.setDaemon(true); return t;
        });
        stats.scheduleAtFixedRate(() ->
                        log.info("STATS pod={} written={} duplicates_dropped={} to_dlt={} infra_retries={}",
                                pod, written.get(), duplicates.get(), toDlt.get(), infraRetries.get()),
                5, 5, TimeUnit.SECONDS);

        consumer.subscribe(List.of(topic), new Rebalance());
        try {
            while (running.get()) {
                ConsumerRecords<String, String> records;
                try {
                    records = consumer.poll(Duration.ofMillis(500));
                } catch (WakeupException e) {
                    continue;
                }
                for (ConsumerRecord<String, String> r : records) {
                    if (!running.get()) break;
                    handle(r);
                    pending.put(new TopicPartition(r.topic(), r.partition()),
                            new OffsetAndMetadata(r.offset() + 1));
                }
                commitPending("batch");
            }
        } catch (WakeupException | StopRequested e) {
            log.info("STOP requested pod={}", pod);
        } finally {
            finalCommit();
            stats.shutdownNow();
            consumer.close(Duration.ofSeconds(10));
            dltProducer.close(Duration.ofSeconds(10));
            sink.close();
            log.info("STOPPED pod={} written={} duplicates_dropped={} to_dlt={}",
                    pod, written.get(), duplicates.get(), toDlt.get());
        }
    }

    // ------------------------------------------------------------------ обработка одного события

    private void handle(ConsumerRecord<String, String> r) {
        Event event = null;
        Exception last = null;
        int attempt = 0;
        while (event == null && attempt < maxAttempts) {
            attempt++;
            try {
                event = process(r);
            } catch (Exception ex) {
                last = ex;
                log.warn("PROCESS_FAILED attempt={}/{} partition={} offset={} cause={}",
                        attempt, maxAttempts, r.partition(), r.offset(), ex);
                if (attempt < maxAttempts) pause(100L * attempt);
            }
        }

        if (event != null) {
            final Event ev = event;
            Sink.Result res = retryForever("sink", r, () -> sink.write(ev, pod, r));
            if (res == Sink.Result.INSERTED) {
                written.incrementAndGet();
            } else {
                duplicates.incrementAndGet();
                log.warn("DUPLICATE_DROPPED event_id={} user_id={} seq={} partition={} offset={}",
                        ev.eventId(), ev.userId(), ev.seq(), r.partition(), r.offset());
            }
        } else {
            final Exception cause = last;
            final int attempts = attempt;
            retryForever("dlt", r, () -> { sendToDlt(r, cause, attempts); return null; });
            toDlt.incrementAndGet();
            log.warn("SENT_TO_DLT partition={} offset={} attempts={} cause={}",
                    r.partition(), r.offset(), attempts, cause);
        }
    }

    private Event process(ConsumerRecord<String, String> r) throws Exception {
        Thread.sleep(delayMs);
        Event e = Json.MAPPER.readValue(r.value(), Event.class);
        if (e.isPoison()) throw new IllegalStateException("poison event " + e.eventId());
        return e;
    }

    private void sendToDlt(ConsumerRecord<String, String> r, Exception cause, int attempts) throws Exception {
        ProducerRecord<String, String> pr =
                new ProducerRecord<>(dltTopic, r.partition(), r.key(), r.value());
        pr.headers()
                .add("dlt.error", utf8(cause.getClass().getName() + ": " + cause.getMessage()))
                .add("dlt.source.topic", utf8(r.topic()))
                .add("dlt.source.partition", utf8(Integer.toString(r.partition())))
                .add("dlt.source.offset", utf8(Long.toString(r.offset())))
                .add("dlt.attempts", utf8(Integer.toString(attempts)))
                .add("dlt.consumer", utf8("consumer-" + pod));
        dltProducer.send(pr).get(30, TimeUnit.SECONDS);
    }


    private <T> T retryForever(String what, ConsumerRecord<?, ?> r, Action<T> action) {
        while (true) {
            try {
                return action.run();
            } catch (Exception ex) {
                infraRetries.incrementAndGet();
                log.warn("INFRA_RETRY what={} partition={} offset={} cause={}",
                        what, r.partition(), r.offset(), ex.toString());
                if (!running.get()) throw new StopRequested();
                pause(1000);
                if (!running.get()) throw new StopRequested();
            }
        }
    }

    // ------------------------------------------------------------------ коммиты и ребаланс

    private void commitPending(String reason) {
        if (pending.isEmpty()) return;
        Map<TopicPartition, OffsetAndMetadata> snapshot = new HashMap<>(pending);
        try {
            consumer.commitSync(snapshot);
            pending.clear();
            log.debug("COMMITTED reason={} offsets={}", reason, offsets(snapshot));
        } catch (CommitFailedException ex) {
            log.warn("COMMIT_FAILED reason={} offsets={} cause={}", reason, offsets(snapshot), ex.toString());
            pending.clear();
        } catch (WakeupException ex) {
            throw ex;
        } catch (Exception ex) {
            log.warn("COMMIT_ERROR reason={} offsets={} cause={}", reason, offsets(snapshot), ex.toString());
        }
    }

    private void finalCommit() {
        for (int i = 0; i < 2; i++) {
            try { commitPending("shutdown"); return; }
            catch (WakeupException ignored) { }
        }
    }

    private final class Rebalance implements ConsumerRebalanceListener {
        @Override public void onPartitionsRevoked(Collection<TopicPartition> parts) {
            Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
            for (TopicPartition tp : parts) {
                OffsetAndMetadata o = pending.remove(tp);
                if (o != null) toCommit.put(tp, o);
            }
            log.info("REBALANCE_REVOKED pod={} partitions={} committing={}", pod, parts, offsets(toCommit));
            if (!toCommit.isEmpty()) {
                try {
                    consumer.commitSync(toCommit);
                } catch (Exception ex) {
                    log.warn("REVOKE_COMMIT_FAILED cause={}", ex.toString());
                }
            }
        }

        @Override public void onPartitionsAssigned(Collection<TopicPartition> parts) {
            Map<String, Long> resume = new TreeMap<>();
            for (TopicPartition tp : parts) resume.put(tp.toString(), consumer.position(tp));
            log.info("REBALANCE_ASSIGNED pod={} partitions={} resume_from={}", pod, parts, resume);
        }

        @Override public void onPartitionsLost(Collection<TopicPartition> parts) {
            pending.keySet().removeAll(parts);
            log.warn("REBALANCE_LOST pod={} partitions={}", pod, parts);
        }
    }

    // ------------------------------------------------------------------ утилиты

    private static Map<String, Long> offsets(Map<TopicPartition, OffsetAndMetadata> m) {
        Map<String, Long> out = new TreeMap<>();
        m.forEach((tp, o) -> out.put(tp.toString(), o.offset()));
        return out;
    }

    private static void pause(long ms) {
        try { Thread.sleep(ms); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new StopRequested(); }
    }

    private static byte[] utf8(String s) { return String.valueOf(s).getBytes(StandardCharsets.UTF_8); }

    private static String env(String name, String def) {
        String v = System.getenv(name);
        return (v == null || v.isBlank()) ? def : v;
    }
}