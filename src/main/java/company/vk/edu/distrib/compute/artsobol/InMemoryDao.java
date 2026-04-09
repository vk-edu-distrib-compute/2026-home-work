package company.vk.edu.distrib.compute.artsobol;

import company.vk.edu.distrib.compute.Dao;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryDao implements Dao<byte[]> {
    private final Map<String, byte[]> storage = new ConcurrentHashMap<>();

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException {
        byte[] value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException();
        }
        return value;
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException {
        storage.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException {
        storage.remove(key);
    }

    @Override
    public void close() {
        // No resources to release
    }
}
