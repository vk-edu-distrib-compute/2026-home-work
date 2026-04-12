package company.vk.edu.distrib.compute.technodamo;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryDao implements Dao<byte[]> {
    private final ConcurrentMap<String, byte[]> storage = new ConcurrentHashMap<>();

    @Override
    public byte[] get(String key) {
        validateKey(key);
        byte[] value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException(key);
        }
        return Arrays.copyOf(value, value.length);
    }

    @Override
    public void upsert(String key, byte[] value) {
        validateKey(key);
        validateValue(value);
        storage.put(key, Arrays.copyOf(value, value.length));
    }

    @Override
    public void delete(String key) {
        validateKey(key);
        storage.remove(key);
    }

    @Override
    public void close() throws IOException {
        storage.clear();
    }

    private static void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be null or empty");
        }
    }

    private static void validateValue(byte[] value) {
        if (Objects.isNull(value)) {
            throw new IllegalArgumentException("Value must not be null");
        }
    }
}
