package company.vk.edu.distrib.compute.maryarta;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.sql.*;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class H2Dao implements Dao<byte []> {
    private final Connection connection;
    private final ConcurrentMap<String, ReentrantReadWriteLock> lockMap;

    public H2Dao(String dbName) {
        try {
            connection = DriverManager.getConnection("jdbc:h2:./data/" + dbName);
            lockMap = new ConcurrentHashMap<>();
            try (Statement st = connection.createStatement()) {
                st.execute("""
                        CREATE TABLE IF NOT EXISTS storage (
                            id VARCHAR(255) PRIMARY KEY,
                            data BLOB
                        )
                        """);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize H2Dao for db: " + dbName, e);
        }
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        if (key.isBlank()) {
            throw new IllegalArgumentException("Key is blank");
        }
        final ReentrantReadWriteLock lock =
                lockMap.computeIfAbsent(key, _ -> new ReentrantReadWriteLock());
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
            throw new RuntimeException(e);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(String key, byte [] value) throws IllegalArgumentException, IOException {
        if (key.isBlank()) {
            throw new IllegalArgumentException("Key is blank");
        }
        final ReentrantReadWriteLock lock =
                lockMap.computeIfAbsent(key, _ -> new ReentrantReadWriteLock());
        lock.writeLock().lock();
        try (PreparedStatement ps = connection.prepareStatement(
                "MERGE INTO storage (id, data) KEY(id) VALUES (?, ?)")) {
            ps.setString(1, key);
            ps.setBytes(2, value);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        if (key.isBlank()) {
            throw new IllegalArgumentException("Key is blank");
        }
        final ReentrantReadWriteLock lock =
                lockMap.computeIfAbsent(key, _ -> new ReentrantReadWriteLock());
        lock.writeLock().lock();
        try (PreparedStatement ps = connection.prepareStatement(
                "DELETE FROM storage WHERE id = ?")) {
            ps.setString(1, key);
            ps.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
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
            throw new RuntimeException(e);
        }
    }

}
