package company.vk.edu.distrib.compute.maryarta;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.sql.*;
import java.util.NoSuchElementException;

public class H2Dao implements Dao<byte []> {
    private  Connection connection;

    public H2Dao(String dbName) {
        try {
            connection = DriverManager.getConnection("jdbc:h2:./data/" + dbName);
            try (Statement st = connection.createStatement()) {
                st.execute("""
                        CREATE TABLE IF NOT EXISTS storage (
                            id VARCHAR(255) PRIMARY KEY,
                            data BLOB
                        )
                        """);
            }
        } catch (SQLException e) {

        }
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        if (key.isBlank()) {
            throw new IllegalArgumentException("Key is blank");
        }
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
        }
    }

    @Override
    public void upsert(String key, byte [] value) throws IllegalArgumentException, IOException {
        if (key.isBlank()) {
            throw new IllegalArgumentException("Key is blank");
        }
        try (PreparedStatement ps = connection.prepareStatement(
                "MERGE INTO storage (id, data) KEY(id) VALUES (?, ?)")) {
            ps.setString(1, key);
            ps.setBytes(2, value);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        if (key.isBlank()) {
            throw new IllegalArgumentException("Key is blank");
        }
        try (PreparedStatement ps = connection.prepareStatement(
                "DELETE FROM storage WHERE id = ?")) {
            ps.setString(1, key);
            ps.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
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
