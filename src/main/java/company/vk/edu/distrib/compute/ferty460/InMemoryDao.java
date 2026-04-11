package company.vk.edu.distrib.compute.ferty460;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryDao implements Dao<byte[]> {

    private final Map<String, byte[]> storage;
    private boolean closed;

    public InMemoryDao() {
        this.storage = new ConcurrentHashMap<>();
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        if (closed) {
            throw new IOException("Dao is closed");
        }

        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be empty");
        }

        byte[] value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException("Key not found: " + key);
        }

        return value.clone();
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        if (closed) {
            throw new IOException("Dao is closed");
        }

        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be empty");
        }
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }

        storage.put(key, value.clone());
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        if (closed) {
            throw new IOException("Dao is closed");
        }

        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be empty");
        }

        storage.remove(key);
    }

    @Override
    public void close() throws IOException {
        closed = true;
        storage.clear();
    }

}
