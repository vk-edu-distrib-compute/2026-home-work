package company.vk.edu.distrib.compute.bushuev_a_s;

import company.vk.edu.distrib.compute.Dao;

import java.util.Map;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class MyDao implements Dao<byte[]> {

    private final Map<String, byte[]> storage = new ConcurrentHashMap<>();

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key is null or blank");
        }
        final var value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException("no value for key" + key);
        }
        return value;
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key is null or blank");
        }
        if (value == null) {
            throw new IllegalArgumentException("value is null");
        }
        storage.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key is null or blank");
        }
        if (!storage.containsKey(key)) {
            throw new IllegalArgumentException("key is not in dao");
        }
        storage.remove(key);
    }

    @Override
    public void close() throws IOException {
        //yet it's empty
    }
}
