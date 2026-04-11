package company.vk.edu.distrib.compute.polozhentsev_ivan;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryDao implements Dao<byte[]> {

    private final Map<String, byte[]> storage = new ConcurrentHashMap<>();

    private static void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("invalid key");
        }
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        validateKey(key);
        byte[] value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException(key);
        }
        return value;
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        validateKey(key);
        storage.put(key, Objects.requireNonNullElse(value, new byte[0]));
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        validateKey(key);
        storage.remove(key);
    }

    @Override
    public void close() throws IOException {
    }
}
