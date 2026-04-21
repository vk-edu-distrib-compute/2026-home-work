package company.vk.edu.distrib.compute.korjick.dao;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.NoSuchElementException;

public class H2Dao implements Dao<byte[]> {
    private static final String CONNECTION_STRING = "jdbc:h2:mem:node_";
    private static final String CONNECTION_PARAM = "DB_CLOSE_DELAY=-1";
    private static final String USERNAME = "test";
    private static final String PASSWORD = "";

    private static final String TABLE_NAME = "storage";
    private static final String KEY_COLUMN = "id";
    private static final String VALUE_COLUMN = "data";

    private static final String CREATE_TABLE_QUERY = String.format(
            "CREATE TABLE IF NOT EXISTS %s (%s TEXT PRIMARY KEY, %s BLOB);",
            TABLE_NAME, KEY_COLUMN, VALUE_COLUMN);
    private static final String SELECT_QUERY = String.format("SELECT %s FROM %s WHERE %s = ?;",
            VALUE_COLUMN, TABLE_NAME, KEY_COLUMN);
    private static final String INSERT_QUERY = String.format("MERGE INTO %s (%s, %s) VALUES (?, ?);",
            TABLE_NAME, KEY_COLUMN, VALUE_COLUMN);
    private static final String DELETE_QUERY = String.format("DELETE FROM %s WHERE %s = ?;",
            TABLE_NAME, KEY_COLUMN);

    private static final Logger log = LoggerFactory.getLogger(H2Dao.class);

    private final HikariDataSource dataSource;

    public H2Dao(int port) throws IOException {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(CONNECTION_STRING + port + ";" + CONNECTION_PARAM);
        config.setUsername(USERNAME);
        config.setPassword(PASSWORD);

        this.dataSource = new HikariDataSource(config);

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(CREATE_TABLE_QUERY);
            log.info("Storage initialized successfully");
        } catch (SQLException e) {
            throw new IOException("Failed to initialize DB schema", e);
        }
    }

    @Override
    public byte[] get(String key) throws IllegalArgumentException, IOException {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key is null or blank");
        }

        try (var connection = dataSource.getConnection();
             var statement = connection.prepareStatement(SELECT_QUERY)) {

            statement.setString(1, key);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getBytes(VALUE_COLUMN);
                }
            }

            throw new NoSuchElementException("No value for key: " + key);
        } catch (SQLException e) {
            throw new IOException("Failed to get value for key: " + key, e);
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key is null or blank");
        }
        try (var connection = dataSource.getConnection();
             var statement = connection.prepareStatement(INSERT_QUERY)) {
            statement.setString(1, key);
            statement.setBytes(2, value);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key is null or blank");
        }
        try (var connection = dataSource.getConnection();
             var statement = connection.prepareStatement(DELETE_QUERY)) {
            statement.setString(1, key);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        this.dataSource.close();
    }
}
