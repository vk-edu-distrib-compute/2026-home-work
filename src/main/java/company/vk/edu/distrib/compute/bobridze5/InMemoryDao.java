package company.vk.edu.distrib.compute.bobridze5;

import company.vk.edu.distrib.compute.Dao;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryDao implements Dao<byte[]> {
    private final ConcurrentMap<String, byte[]> storage = new ConcurrentHashMap<>();

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException {
        validateKey(key);
        byte[] value = storage.get(key);

        if (value == null) {
            throw new NoSuchElementException("Value not found for key: " + key);
        }

        return Arrays.copyOf(value, value.length);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException {
        validateKey(key);
        validateValue(value);

        storage.put(key, Arrays.copyOf(value, value.length));
    }

    @Override
    public void delete(String key) throws IllegalArgumentException {
        validateKey(key);

        storage.remove(key);
    }

    @Override
    public void close() {
        storage.clear();
    }

    private void validateKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key must not be null or blank");
        }
    }

    private void validateValue(byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException("Value must not be null");
        }
    }
}
