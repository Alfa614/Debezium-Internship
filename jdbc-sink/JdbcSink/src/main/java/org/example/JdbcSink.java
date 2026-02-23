package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import javax.enterprise.context.Dependent;
import javax.inject.Named;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.ChangeConsumer;
import io.debezium.engine.DebeziumEngine.RecordCommitter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;

/**
 * Custom JDBC sink for Debezium Server.
 * Parses Debezium JSON events and applies them to PostgreSQL.
 */
@Dependent
@Named("jdbc")
public class JdbcSink implements ChangeConsumer<ChangeEvent<String, String>> {

    private final Connection connection;
    private final ObjectMapper mapper = new ObjectMapper();

    public JdbcSink() {
        try {
            // Hardcode for now (we can move to properties later)
            String url = "jdbc:postgresql://localhost:5432/postgres";
            String user = "postgres";
            String password = "Alfaiz123";

            connection = DriverManager.getConnection(url, user, password);
            connection.setAutoCommit(false);

            System.out.println("Connected to PostgreSQL");

        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to PostgreSQL", e);
        }
    }

    @Override
    public void handleBatch(
            List<ChangeEvent<String, String>> events,
            RecordCommitter<ChangeEvent<String, String>> committer
    ) throws InterruptedException {

        try {
            for (ChangeEvent<String, String> event : events) {

                String payload = event.value();
                if (payload == null) continue;

                System.out.println("Debezium Event:");
                System.out.println(payload);

                JsonNode root = mapper.readTree(payload);

                String op = root.get("op").asText();
                String table = root.get("source").get("table").asText();

                String sql = null;

                if ("c".equals(op) || "u".equals(op)) {
                    JsonNode after = root.get("after");
                    sql = buildUpsertSql(table, after);

                } else if ("d".equals(op)) {
                    JsonNode before = root.get("before");
                    sql = buildDeleteSql(table, before);
                }

                if (sql != null) {
                    System.out.println("Executing: " + sql);

                    try (Statement stmt = connection.createStatement()) {
                        stmt.executeUpdate(sql);
                    }
                }

                long ts = root.get("ts_ms").asLong();
                String pk = extractPrimaryKey(root);

                updateCheckpoint(table, pk, ts);

                committer.markProcessed(event);
            }

            connection.commit();
            committer.markBatchFinished();

        } catch (Exception e) {
            e.printStackTrace();

            try {
                connection.rollback();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    private String buildUpsertSql(String table, JsonNode after) {

        StringBuilder cols = new StringBuilder();
        StringBuilder vals = new StringBuilder();
        StringBuilder updates = new StringBuilder();

        after.fieldNames().forEachRemaining(col -> {

            String val = after.get(col).isNull()
                    ? "NULL"
                    : "'" + after.get(col).asText() + "'";

            if (cols.length() > 0) {
                cols.append(", ");
                vals.append(", ");
                updates.append(", ");
            }

            cols.append(col);
            vals.append(val);
            updates.append(col).append(" = EXCLUDED.").append(col);
        });

        return "INSERT INTO " + table +
                " (" + cols + ") VALUES (" + vals + ") " +
                "ON CONFLICT (id) DO UPDATE SET " +
                updates + ";";
    }

    private String buildDeleteSql(String table, JsonNode before) {

        String idVal = before.get("id").asText();

        return "DELETE FROM " + table +
                " WHERE id = '" + idVal + "';";
    }

    private void updateCheckpoint(String table, String pk, long ts) throws Exception {

        String sql =
                "INSERT INTO cdc_checkpoint " +
                        "(table_name, last_synced_pk, last_synced_ts) " +
                        "VALUES (?, ?, ?) " +
                        "ON CONFLICT (table_name) DO UPDATE SET " +
                        "last_synced_pk = EXCLUDED.last_synced_pk, " +
                        "last_synced_ts = EXCLUDED.last_synced_ts;";

        try (PreparedStatement stmt =
                     connection.prepareStatement(sql)) {

            stmt.setString(1, table);
            stmt.setString(2, pk);
            stmt.setLong(3, ts);

            stmt.executeUpdate();
        }
    }

    private String extractPrimaryKey(JsonNode root) {

        JsonNode after = root.get("after");

        if (after != null && after.has("id")) {
            return after.get("id").asText();
        }

        JsonNode before = root.get("before");

        if (before != null && before.has("id")) {
            return before.get("id").asText();
        }

        return null;
    }
}
