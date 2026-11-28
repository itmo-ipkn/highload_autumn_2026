import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.*;
import java.time.ZoneOffset;

public final class Sink implements AutoCloseable {
    public enum Result { INSERTED, DUPLICATE }

    private static final String INSERT = """
            INSERT INTO sink_events(event_id, user_id, seq, type, produced_at,
                                    consumer_pod, src_topic, src_partition, src_offset)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (event_id) DO NOTHING
            """;
    private static final String DUP = """
            INSERT INTO sink_counters(name, value) VALUES (?, 1)
            ON CONFLICT (name) DO UPDATE SET value = sink_counters.value + 1
            """;

    private final String url, user, password;
    private Connection conn;

    public Sink(String url, String user, String password) {
        this.url = url; this.user = user; this.password = password;
    }

    private Connection conn() throws SQLException {
        if (conn == null || conn.isClosed() || !conn.isValid(2)) {
            closeQuietly();
            conn = DriverManager.getConnection(url, user, password);
            conn.setAutoCommit(false);
        }
        return conn;
    }

    public Result write(Event e, int pod, ConsumerRecord<?, ?> src) throws SQLException {
        Connection c = conn();
        try {
            int rows;
            try (PreparedStatement ps = c.prepareStatement(INSERT)) {
                ps.setObject(1, e.eventId());
                ps.setObject(2, e.userId());
                ps.setLong(3, e.seq());
                ps.setString(4, e.type().getType());
                ps.setObject(5, e.producedAt().atOffset(ZoneOffset.UTC));
                ps.setInt(6, pod);
                ps.setString(7, src.topic());
                ps.setInt(8, src.partition());
                ps.setLong(9, src.offset());
                rows = ps.executeUpdate();
            }
            if (rows == 0) {
                try (PreparedStatement ps = c.prepareStatement(DUP)) {
                    ps.setString(1, "duplicates:consumer-" + pod);
                    ps.executeUpdate();
                }
            }
            c.commit();
            return rows == 1 ? Result.INSERTED : Result.DUPLICATE;
        } catch (SQLException ex) {
            try { c.rollback(); } catch (SQLException ignored) { }
            closeQuietly();
            throw ex;
        }
    }

    private void closeQuietly() {
        try { if (conn != null) conn.close(); } catch (SQLException ignored) { }
        conn = null;
    }

    @Override public void close() { closeQuietly(); }
}
