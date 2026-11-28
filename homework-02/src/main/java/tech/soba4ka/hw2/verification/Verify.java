package tech.soba4ka.hw2.verification;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import tech.soba4ka.hw2.config.Database;
import tech.soba4ka.hw2.config.KafkaConfiguration;
import tech.soba4ka.hw2.model.Event;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.time.Duration;

@Slf4j
public class Verify {
    private static final String INSERT_DEAD_LETTER_SQL =
            "INSERT INTO dead VALUES(?,?,?) ON CONFLICT DO NOTHING";
    private static final String CREATE_DEAD_LETTER_TABLE_SQL =
            "CREATE TEMP TABLE dead(event_id uuid PRIMARY KEY,user_id text,seq bigint)";
    private static final String CREATE_SEEN_EVENTS_VIEW_SQL =
            "CREATE TEMP VIEW seen AS SELECT event_id,user_id,seq FROM sink UNION ALL SELECT * FROM dead";
    private static final String CHECK_USER_SEQUENCES_SQL = """
            SELECT g.user_id,g.seq,count(DISTINCT s.seq) AS found
            FROM generated g LEFT JOIN seen s ON s.user_id=g.user_id AND s.seq BETWEEN 1 AND g.seq
            GROUP BY g.user_id,g.seq ORDER BY g.user_id
            """;
    private static final String READ_VERIFICATION_TOTALS_SQL = """
            SELECT (SELECT count(*) FROM sink), (SELECT count(*) FROM dead),
              (SELECT coalesce(sum(total),0) FROM duplicates),
              (SELECT count(*) FROM sink a JOIN sink b ON a.user_id=b.user_id AND a.seq>b.seq AND a.write_no<b.write_no),
              (SELECT count(*) FROM (SELECT user_id,seq FROM seen GROUP BY user_id,seq HAVING count(*)>1) x)
              + (SELECT count(*) FROM seen s LEFT JOIN generated g USING(user_id)
                 WHERE g.user_id IS NULL OR s.seq NOT BETWEEN 1 AND g.seq)
            """;

    private static final ObjectMapper mapper = new ObjectMapper();

    static String header(ConsumerRecord<String, String> r, String name) {
        var h = r.headers().lastHeader(name);
        return h == null ? "" : new String(h.value(), StandardCharsets.UTF_8);
    }

    static void readDeadLetters(Connection db) throws Exception {
        try (var consumer = KafkaConfiguration.reader(KafkaConfiguration.readerProperties());
             var add = db.prepareStatement(INSERT_DEAD_LETTER_SQL)) {
            var parts = consumer.partitionsFor("events.dlt").stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition())).toList();
            consumer.assign(parts);
            var ends = consumer.endOffsets(parts);
            consumer.seekToBeginning(parts);
            while (parts.stream().anyMatch(p -> consumer.position(p) < ends.get(p))) {
                for (var r : consumer.poll(Duration.ofSeconds(1))) {
                    if (r.offset() >= ends.get(new TopicPartition(r.topic(), r.partition()))) continue;
                    var e = mapper.readValue(r.value(), Event.class);
                    if (!e.poison() || header(r, "reason").isBlank() || !header(r, "topic").equals("events")
                            || Integer.parseInt(header(r, "partition")) != r.partition() ||
                            Long.parseLong(header(r, "offset")) < 0)
                        throw new IllegalStateException("Invalid DLT record " + r.partition() + "@" + r.offset());
                    add.setObject(1, e.event_id());
                    add.setString(2, e.user_id());
                    add.setLong(3, e.seq());
                    add.addBatch();
                }
                add.executeBatch();
            }
        }
    }

    public static void run() throws Exception {
        try (var db = Database.open(); var sql = db.createStatement()) {
            sql.execute(CREATE_DEAD_LETTER_TABLE_SQL);
            readDeadLetters(db);
            sql.execute(CREATE_SEEN_EVENTS_VIEW_SQL);
            long gaps = 0;
            try (var rows = sql.executeQuery(CHECK_USER_SEQUENCES_SQL)) {
                while (rows.next()) {
                    long missing = rows.getLong("seq") - rows.getLong("found");
                    gaps += missing;
                    log.info("{} seq=1..{} missing={}", rows.getString(1), rows.getLong(2), missing);
                }
            }
            try (var r = sql.executeQuery(READ_VERIFICATION_TOTALS_SQL)) {
                r.next();
                log.info("sink={} dlt={} missing={} duplicates={} order_inversions={} conflicts={}",
                         r.getLong(1), r.getLong(2), gaps, r.getLong(3), r.getLong(4), r.getLong(5));
                if (gaps != 0 || r.getLong(5) != 0) throw new IllegalStateException("Incomplete or inconsistent data");
            }
        }
    }
}
