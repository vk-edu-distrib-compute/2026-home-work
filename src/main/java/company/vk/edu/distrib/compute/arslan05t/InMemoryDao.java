package company.vk.edu.distrib.compute.arslan05t;

import company.vk.edu.distrib.compute.Dao;
import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryDao implements Dao<byte[]> {
    private final Map<String, byte[]> storage = new ConcurrentHashMap<>();

    @Override
    public byte[] get(String key) throws NoSuchElementException {
        byte[] value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException("Key not found: " + key);
        }
        return value;
    }

    @Override
    public void upsert(String key, byte[] value) {
        storage.put(key, value);
    }

    @Override
    public void delete(String key) {
        storage.remove(key);
    }

    @Override
    public void close() throws IOException {
        // Nothing to close for in-memory storage
    }
}
