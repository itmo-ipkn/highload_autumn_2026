package tech.soba4ka.hw2.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import tech.soba4ka.hw2.config.Database;
import tech.soba4ka.hw2.config.KafkaConfiguration;
import tech.soba4ka.hw2.model.Event;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

@Slf4j
public class ProcessEvents {
    private static final String LOCK_USER_SQL =
            "SELECT pg_advisory_xact_lock(hashtext(?))";
    private static final String INSERT_EVENT_SQL =
            "INSERT INTO sink(event_id,user_id,seq,pod,topic,partition_id,kafka_offset) VALUES(?,?,?,?,?,?,?) ON CONFLICT(event_id) DO NOTHING";
    private static final String COUNT_DUPLICATE_SQL =
            "INSERT INTO duplicates VALUES(?,1) ON CONFLICT(pod) DO UPDATE SET total=duplicates.total+1";

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void run(int pod, BooleanSupplier stopping) throws Exception {
        int delay = Integer.parseInt(System.getenv().getOrDefault("PROCESSING_DELAY_MS", "20"));
        var config = KafkaConfiguration.readerProperties();
        config.put("group.protocol", "classic");
        config.put("group.id", "events-processor");
        config.put("client.id", "c" + pod);
        config.put("max.poll.records", 12);
        config.put("session.timeout.ms", 10000);
        config.put("heartbeat.interval.ms", 2500);
        try (var db = Database.open(); var kafka = KafkaConfiguration.producer(false);
             var client = KafkaConfiguration.reader(config)) {
            db.setAutoCommit(false);
            var ready = new HashMap<TopicPartition, OffsetAndMetadata>();
            client.subscribe(List.of("events"), new ConsumerRebalanceListener() {
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    for (var tp : partitions)
                        log.info("assigned {} resume={}", tp, client.position(tp));
                }

                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    var revoked = new HashMap<>(ready);
                    revoked.keySet().retainAll(partitions);
                    if (!revoked.isEmpty()) client.commitSync(revoked);
                    ready.keySet().removeAll(partitions);
                    log.info("revoked {}", partitions);
                }

                public void onPartitionsLost(Collection<TopicPartition> partitions) {
                    ready.keySet().removeAll(partitions);
                }
            });
            while (!stopping.getAsBoolean()) {
                for (var record : client.poll(Duration.ofMillis(500))) {
                    Event event = mapper.readValue(record.value(), Event.class);
                    for (int attempt = 1; true; attempt++) {
                        Thread.sleep(delay);
                        try {
                            if (event.poison()) throw new IllegalArgumentException("poison flag");
                        } catch (IllegalArgumentException e) {
                            log.warn("Failed {}/{}@{} attempt={}", record.topic(), record.partition(), record.offset(),
                                     attempt);
                            if (attempt < 3) continue;
                            var dead = new ProducerRecord<>("events.dlt", record.partition(),
                                                            record.key(), record.value());
                            var headers = Map.of("reason", e.toString(), "topic", record.topic(), "partition",
                                                 String.valueOf(record.partition()), "offset",
                                                 String.valueOf(record.offset()));
                            headers.forEach((k, v) -> dead.headers().add(k, v.getBytes(StandardCharsets.UTF_8)));
                            kafka.send(dead).get();
                            break;
                        }
                        save(db, pod, record, event);
                        break;
                    }
                    ready.put(new TopicPartition(record.topic(), record.partition()),
                              new OffsetAndMetadata(record.offset() + 1));
                }
                if (!ready.isEmpty()) {
                    client.commitSync(ready);
                    ready.clear();
                }
            }
        }
    }

    private static void save(Connection db, int pod, ConsumerRecord<String, String> record, Event event) throws SQLException {
        try (var lock = db.prepareStatement(LOCK_USER_SQL);
             var insert = db.prepareStatement(INSERT_EVENT_SQL);
             var duplicate = db.prepareStatement(COUNT_DUPLICATE_SQL)) {
            // Serialize writes per user so the generated insertion index follows commit order.
            lock.setString(1, event.user_id());
            lock.execute();
            insert.setObject(1, event.event_id());
            insert.setString(2, event.user_id());
            insert.setLong(3, event.seq());
            insert.setInt(4, pod);
            insert.setString(5, record.topic());
            insert.setInt(6, record.partition());
            insert.setLong(7, record.offset());
            if (insert.executeUpdate() == 0) {
                duplicate.setInt(1, pod);
                duplicate.executeUpdate();
            }
            db.commit();
        }
    }
}
