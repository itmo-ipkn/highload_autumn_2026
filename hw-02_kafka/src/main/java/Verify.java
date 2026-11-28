import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.*;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.*;

public final class Verify {

    public static void main(String[] args) throws Exception {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
        System.out.println(env("GITHUB_LOGIN", "unknown") + " " + OffsetDateTime.now().withNano(0));

        Map<UUID, BitSet> seen = new HashMap<>();
        long sinkRows = 0, duplicates = 0, inversions = 0;

        try (Connection c = DriverManager.getConnection(
                env("DB_URL", "jdbc:postgresql://postgres:5432/sink"), "sink", "sink");
             Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT user_id, seq FROM sink_events")) {
                while (rs.next()) {
                    sinkRows++;
                    seen.computeIfAbsent(rs.getObject(1, UUID.class), k -> new BitSet()).set((int) rs.getLong(2));
                }
            }
            try (ResultSet rs = st.executeQuery("""
                    SELECT count(*) FROM (
                      SELECT seq, max(seq) OVER (PARTITION BY user_id ORDER BY write_id
                             ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS m
                      FROM sink_events) t WHERE m > seq""")) {
                rs.next(); inversions = rs.getLong(1);
            }
            try (ResultSet rs = st.executeQuery(
                    "SELECT coalesce(sum(value), 0) FROM sink_counters WHERE name LIKE 'duplicates:%'")) {
                rs.next(); duplicates = rs.getLong(1);
            }
        }

        Properties p = new Properties();
        p.put("bootstrap.servers", env("BOOTSTRAP_SERVERS", "kafka-1:19092,kafka-2:19092,kafka-3:19092"));
        p.put("key.deserializer", StringDeserializer.class.getName());
        p.put("value.deserializer", StringDeserializer.class.getName());
        p.put("enable.auto.commit", "false");
        Set<UUID> dlt = new HashSet<>();
        long eventsTotal = 0;
        try (KafkaConsumer<String, String> kc = new KafkaConsumer<>(p)) {
            List<TopicPartition> dp = parts(kc, "events.dlt");
            kc.assign(dp);
            kc.seekToBeginning(dp);
            Map<TopicPartition, Long> end = kc.endOffsets(dp);
            long deadline = System.currentTimeMillis() + 60_000;
            while (dp.stream().anyMatch(tp -> kc.position(tp) < end.get(tp)) && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> r : kc.poll(Duration.ofMillis(300))) {
                    Event e = Json.MAPPER.readValue(r.value(), Event.class);
                    if (dlt.add(e.eventId())) {
                        seen.computeIfAbsent(e.userId(), k -> new BitSet()).set((int) e.seq());
                    }
                }
            }
            for (long v : kc.endOffsets(parts(kc, "events")).values()) eventsTotal += v;
        }

        long missing = 0, gapUsers = 0;
        for (BitSet bs : seen.values()) {
            int miss = bs.length() - 1 - bs.cardinality();
            if (miss > 0) { missing += miss; gapUsers++; }
        }

        System.out.println("== VERIFY ==");
        System.out.println("events_in_topic=" + eventsTotal + " sink_rows=" + sinkRows + " dlt_unique=" + dlt.size()
                + (eventsTotal == sinkRows + dlt.size() ? "  (баланс сходится)" : "  (баланс НЕ сходится)"));
        System.out.println("[1] Пропуски seq (sink + events.dlt): " + missing + " у пользователей: " + gapUsers);
        System.out.println("[2] Отброшено дублей (повторных доставок): " + duplicates);
        System.out.println("[3] Нарушения порядка в sink: " + inversions);
        System.exit(missing == 0 ? 0 : 1);
    }

    private static List<TopicPartition> parts(KafkaConsumer<?, ?> c, String topic) {
        return c.partitionsFor(topic).stream().map(i -> new TopicPartition(topic, i.partition())).toList();
    }

    private static String env(String name, String def) {
        String v = System.getenv(name);
        return (v == null || v.isBlank()) ? def : v;
    }
}