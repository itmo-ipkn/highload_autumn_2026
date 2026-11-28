import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public final class ProducerApp {
    private static final Logger log = LoggerFactory.getLogger(ProducerApp.class);

    private static final AtomicLong generated = new AtomicLong();
    private static final AtomicLong acked = new AtomicLong();
    private static final AtomicLong failed = new AtomicLong();

    public static void main(String[] args) throws Exception {
        String bootstrap = env("BOOTSTRAP_SERVERS", "kafka-1:19092,kafka-2:19092,kafka-3:19092");
        String topic     = env("TOPIC", "events");
        int podIndex     = Integer.parseInt(env("POD_INDEX", "0"));
        int podCount     = Integer.parseInt(env("POD_COUNT", "3"));
        int totalUsers   = Integer.parseInt(env("TOTAL_USERS", "1000"));
        int ratePerSec   = Integer.parseInt(env("RATE_PER_SEC", "200"));
        double poison    = Double.parseDouble(env("POISON_RATE", "0.02"));
        long seed        = Long.parseLong(env("SEED", "42")) + podIndex;
        String keyMode   = env("KEY_MODE", "user_id");
        if (!keyMode.equals("user_id") && !keyMode.equals("none")) {
            throw new IllegalArgumentException("KEY_MODE must be user_id|none, got " + keyMode);
        }
        String deliveryTimeoutMs = env("DELIVERY_TIMEOUT_MS", "20000");
        String requestTimeoutMs  = env("REQUEST_TIMEOUT_MS", "5000");
        String maxBlockMs        = env("MAX_BLOCK_MS", "10000");

        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-" + podIndex);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        p.put(ProducerConfig.ACKS_CONFIG, "all");
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        p.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        p.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
        p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs);

        p.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        p.put(ProducerConfig.BATCH_SIZE_CONFIG, env("BATCH_SIZE", "32768"));

        EventGenerator gen = new EventGenerator(podIndex, podCount, totalUsers, poison, seed);
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);

        AtomicBoolean running = new AtomicBoolean(true);
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            try { mainThread.join(); }
            catch (InterruptedException ignored) { }
        }, "shutdown-hook"));

        ScheduledExecutorService stats = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "stats"); t.setDaemon(true); return t;
        });
        stats.scheduleAtFixedRate(() ->
                        log.info("STATS pod={} generated={} acked={} failed={} in_flight={}",
                                podIndex, generated.get(), acked.get(), failed.get(),
                                generated.get() - acked.get() - failed.get()),
                5, 5, TimeUnit.SECONDS);

        log.info("START pod={}/{} topic={} rate={}/s key_mode={} delivery_timeout_ms={} bootstrap={}",
                podIndex, podCount, topic, ratePerSec, keyMode, deliveryTimeoutMs, bootstrap);

        final long intervalNs = 1_000_000_000L / ratePerSec;
        long next = System.nanoTime();
        while (running.get()) {
            send(producer, topic, gen.next(), keyMode);

            next += intervalNs;
            long sleep = next - System.nanoTime();
            if (sleep > 0) {
                LockSupport.parkNanos(sleep);
            } else if (sleep < -1_000_000_000L) {
                next = System.nanoTime();
            }
        }

        log.info("SHUTDOWN pod={} флашим буфер...", podIndex);
        producer.close(Duration.ofSeconds(20));
        log.info("STOPPED pod={} generated={} acked={} failed={}",
                podIndex, generated.get(), acked.get(), failed.get());
    }

    private static void send(KafkaProducer<String, String> producer, String topic,
                             Event e, String keyMode) {
        generated.incrementAndGet();
        String key = keyMode.equals("user_id") ? e.userId().toString() : null;
        String value;
        try {
            value = Json.MAPPER.writeValueAsString(e);
        } catch (JsonProcessingException ex) {
            failed.incrementAndGet();
            logFailure(e, ex);
            return;
        }
        try {
            producer.send(new ProducerRecord<>(topic, key, value), (md, ex) -> {
                if (ex == null) acked.incrementAndGet();
                else { failed.incrementAndGet(); logFailure(e, ex); }
            });
        } catch (Exception ex) {
            failed.incrementAndGet();
            logFailure(e, ex);
        }
    }

    private static void logFailure(Event e, Exception ex) {
        log.error("SEND_FAILED event_id={} user_id={} seq={} type={} cause={}",
                e.eventId(), e.userId(), e.seq(), e.type().getType(), ex.toString());
    }

    private static String env(String name, String def) {
        String v = System.getenv(name);
        return (v == null || v.isBlank()) ? def : v;
    }
}