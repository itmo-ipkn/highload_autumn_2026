package tech.soba4ka.hw2.producer;

import lombok.extern.slf4j.Slf4j;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import tech.soba4ka.hw2.config.Database;
import tech.soba4ka.hw2.config.KafkaConfiguration;
import tech.soba4ka.hw2.model.Event;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

@Slf4j
public class Generate {
    private static final String INITIALIZE_USER_SQL =
            "INSERT INTO generated VALUES (?,0,NULL) ON CONFLICT DO NOTHING";
    private static final String READ_USER_STATE_SQL =
            "SELECT seq,pending FROM generated WHERE user_id=?";
    private static final String SAVE_PENDING_EVENT_SQL =
            "UPDATE generated SET pending=? WHERE user_id=?";
    private static final String ACKNOWLEDGE_EVENT_SQL =
            "UPDATE generated SET seq=seq+1,pending=NULL WHERE user_id=?";

    private static final ObjectMapper mapper = new ObjectMapper();

    private static final String[] TYPES = {"page_view", "add_to_cart", "purchase"};

    public static void run(int pod, BooleanSupplier stopping) throws Exception {
        boolean keyed = switch (System.getenv().getOrDefault("KEY_MODE", "user_id")) {
            case "user_id" -> true;
            case "none" -> false;
            default -> throw new IllegalArgumentException("KEY_MODE");
        };
        int rate = Integer.parseInt(System.getenv().getOrDefault("RATE", "24"));
        if (rate < 1) throw new IllegalArgumentException("RATE must be positive");
        try (var kafka = KafkaConfiguration.producer(!keyed);
             var db = Database.open();
             var init = db.prepareStatement(INITIALIZE_USER_SQL);
             var read = db.prepareStatement(READ_USER_STATE_SQL);
             var stage = db.prepareStatement(SAVE_PENDING_EVENT_SQL);
             var ack = db.prepareStatement(ACKNOWLEDGE_EVENT_SQL)) {
            int index = 0;
            while (!stopping.getAsBoolean()) {
                long start = System.nanoTime();
                String user = "p" + pod + "-u" + (index++ % 4);
                init.setString(1, user);
                init.executeUpdate();
                read.setString(1, user);
                String body;
                try (var row = read.executeQuery()) {
                    row.next();
                    body = row.getString("pending");
                    if (body == null) {
                        var rng = ThreadLocalRandom.current();
                        var event = new Event(UUID.randomUUID(), user, TYPES[rng.nextInt(TYPES.length)],
                                              row.getLong("seq") + 1, Instant.now().toString(),
                                              Map.of("poison", rng.nextInt(100) < 2));
                        body = mapper.writeValueAsString(event);
                        stage.setString(1, body);
                        stage.setString(2, user);
                        stage.executeUpdate();
                    }
                }
                while (true) {
                    try {
                        kafka.send(new ProducerRecord<>("events", keyed ? user : null, body)).get();
                        break;
                    } catch (ExecutionException | org.apache.kafka.common.KafkaException e) {
                        log.error("Send failed, pending event retained", e);
                        if (stopping.getAsBoolean()) return;
                        Thread.sleep(500);
                    }
                }
                ack.setString(1, user);
                ack.executeUpdate();
                long remaining = 1_000_000_000L / rate - (System.nanoTime() - start);
                if (remaining > 0) TimeUnit.NANOSECONDS.sleep(remaining);
            }
            kafka.flush();
        }
    }
}
