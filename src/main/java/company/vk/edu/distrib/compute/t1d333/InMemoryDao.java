package company.vk.edu.distrib.compute.t1d333;

import company.vk.edu.distrib.compute.Dao;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryDao implements Dao<byte[]> {
    private final Map<String, byte[]> data = new ConcurrentHashMap<>();

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }

        byte[] value = data.get(key);
        if (value == null) {
            throw new NoSuchElementException("Key not found: " + key);
        }
        return value;
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }

        data.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }

        data.remove(key);
    }

    @Override
    public void close() {
        data.clear();
    }
}
