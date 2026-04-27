package company.vk.edu.distrib.compute.maryarta;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.maryarta.replication.StoredRecord;

import java.io.IOException;
import java.sql.*;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class H2Dao implements Dao<byte []> {
    private Connection connection;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    public String dbName;

    public H2Dao(String dbName) {
        this.dbName = dbName;
        try {
            connection = DriverManager.getConnection("jdbc:h2:./data/" + dbName);
            try (Statement st = connection.createStatement()) {
                st.execute("""
                        CREATE TABLE IF NOT EXISTS storage (
                            id VARCHAR(255) PRIMARY KEY,
                            data BLOB,
                            version BIGINT NOT NULL,
                            deleted BOOLEAN NOT NULL DEFAULT FALSE
                        )
                        """);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to initialize H2Dao for db: " + dbName, e);
        }
    }

    public void start() {
        try {
            if (connection != null && !connection.isClosed()) {
                return;
            }
            connection = DriverManager.getConnection("jdbc:h2:./data/" + dbName);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to start H2Dao for db: " + dbName, e);
        }
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        if (key.isBlank()) {
            throw new IllegalArgumentException("Key is blank");
        }
        lock.readLock().lock();
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT data FROM storage WHERE id = ?")) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new NoSuchElementException("Key not found: " + key);
                }
                return rs.getBytes(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to read value for key: " + key, e);
        } finally {
            lock.readLock().unlock();
        }
    }

    public StoredRecord getRecord(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key is blank");
        }
        lock.readLock().lock();
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT data, version, deleted FROM storage WHERE id = ?")) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                byte[] data = rs.getBytes("data");
                long version = rs.getLong("version");
                boolean deleted = rs.getBoolean("deleted");
                return new StoredRecord(data, version, deleted);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to read value for key: " + key, e);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(String key, byte [] value) throws IllegalArgumentException, IOException {
        if (key.isBlank()) {
            throw new IllegalArgumentException("Key is blank");
        }
        lock.writeLock().lock();
        try (PreparedStatement ps = connection.prepareStatement(
                "MERGE INTO storage (id, data) KEY(id) VALUES (?, ?)")) {
            ps.setString(1, key);
            ps.setBytes(2, value);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to upsert value for key: " + key, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void upsert(String key, byte[] value, long version, boolean deleted) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key is blank");
        }
        lock.writeLock().lock();
        try {
            long existingVersion = Long.MIN_VALUE;
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT version FROM storage WHERE id = ?")) {
                select.setString(1, key);
                try (ResultSet rs = select.executeQuery()) {
                    if (rs.next()) {
                        existingVersion = rs.getLong("version");
                    }
                }
            }
            if (version < existingVersion) {
                return;
            }
            try (PreparedStatement ps = connection.prepareStatement(
                    """
                    MERGE INTO storage (id, data, version, deleted)
                    KEY(id)
                    VALUES (?, ?, ?, ?)
                    """
            )) {
                ps.setString(1, key);
                ps.setBytes(2, value);
                ps.setLong(3, version);
                ps.setBoolean(4, deleted);
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to upsert value for key: " + key, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        if (key.isBlank()) {
            throw new IllegalArgumentException("Key is blank");
        }
        lock.writeLock().lock();
        try (PreparedStatement ps = connection.prepareStatement(
                "DELETE FROM storage WHERE id = ?")) {
            ps.setString(1, key);
            ps.execute();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to delete value for key: " + key, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void delete(String key, long version) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key is blank");
        }
        lock.writeLock().lock();
        try {
            long existingVersion = Long.MIN_VALUE;
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT version FROM storage WHERE id = ?")) {
                select.setString(1, key);
                try (ResultSet rs = select.executeQuery()) {
                    if (rs.next()) {
                        existingVersion = rs.getLong("version");
                    }
                }
            }
            if (version < existingVersion) {
                return;
            }
            try (PreparedStatement ps = connection.prepareStatement(
                    """
                    MERGE INTO storage (id, data, version, deleted)
                    KEY(id)
                    VALUES (?, ?, ?, ?)
                    """
            )) {
                ps.setString(1, key);
                ps.setBytes(2, null);
                ps.setLong(3, version);
                ps.setBoolean(4, true);
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to delete value for key: " + key, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to close H2 connection", e);
        }
    }

}
