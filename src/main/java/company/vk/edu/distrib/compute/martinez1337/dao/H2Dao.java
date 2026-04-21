package company.vk.edu.distrib.compute.martinez1337.dao;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.NoSuchElementException;

public class H2Dao implements Dao<byte[]> {
    private static final Logger log = LoggerFactory.getLogger(H2Dao.class);
    private static final String TABLE_NAME = "kv_store";
    private final HikariDataSource dataSource;

    public H2Dao(String dbUrl) throws IOException {
        try {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(dbUrl);
            config.setMaximumPoolSize(5);
            config.setMinimumIdle(2);
            this.dataSource = new HikariDataSource(config);
            initializeTable();
        } catch (Exception e) {
            log.error("Failed to initialize H2Dao", e);
            throw new IOException("Failed to initialize H2Dao", e);
        }
    }

    private void initializeTable() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement(
                    "CREATE TABLE IF NOT EXISTS "
                            + TABLE_NAME
                            + " ("
                            + "  id VARCHAR(255) PRIMARY KEY,"
                            + "  val BLOB NOT NULL"
                            + ")"
            )) {
                stmt.executeUpdate();
            }
        }
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        validateKey(key);

        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement(
                    "SELECT val FROM " + TABLE_NAME + " WHERE id = ?"
            )) {
                stmt.setString(1, key);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        return rs.getBytes("val");
                    }
                }
            }
        } catch (SQLException e) {
            log.error("Error getting value for key: {}", key, e);
            throw new IOException("Failed to get value from database", e);
        }

        throw new NoSuchElementException("Key not found: " + key);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        validateKey(key);
        validateValue(value);

        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement(
                    "MERGE INTO " + TABLE_NAME + " (id, val) KEY (id) VALUES (?, ?)"
            )) {
                stmt.setString(1, key);
                stmt.setBytes(2, value);
                stmt.executeUpdate();
            }
        } catch (SQLException e) {
            log.error("Error upserting key: {}", key, e);
            throw new IOException("Failed to upsert into database", e);
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        validateKey(key);

        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement(
                    "DELETE FROM " + TABLE_NAME + " WHERE id = ?"
            )) {
                stmt.setString(1, key);
                stmt.executeUpdate();
            }
        } catch (SQLException e) {
            log.error("Error deleting key: {}", key, e);
            throw new IOException("Failed to delete from database", e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (dataSource != null && !dataSource.isClosed()) {
                dataSource.close();
            }
        } catch (Exception e) {
            log.error("Error closing H2Dao", e);
            throw new IOException("Failed to close database connection pool", e);
        }
    }

    private static void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
    }

    private static void validateValue(byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
    }
}
