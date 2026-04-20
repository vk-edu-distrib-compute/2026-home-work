package company.vk.edu.distrib.compute.andrey1af.dao;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryDao implements Dao<byte[]> {

    private final Map<String, byte[]> storage = new ConcurrentHashMap<>();

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        checkKey(key);
        final var value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException("Key not found: " + key);
        }
        return value;
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        checkKey(key);
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        storage.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        checkKey(key);
        storage.remove(key);
    }

    @Override
    public void close() throws IOException {
        // no-op: this DAO does not hold external resources
    }

    private void checkKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key cannot be null or blank");
        }
    }
}
