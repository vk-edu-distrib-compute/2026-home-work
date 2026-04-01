package company.vk.edu.distrib.compute.d1gitale;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryDao implements Dao<byte[]> {
    private final Map<String, byte[]> data = new ConcurrentHashMap<>();

    @Override
    public byte[] get(String key) throws NoSuchElementException {
        if (!data.containsKey(key)) {
            throw new NoSuchElementException("Key not found: " + key);
        }
        return data.get(key);
    }

    @Override
    public void upsert(String key, byte[] value) {
        data.put(key, value);
    }

    @Override
    public void delete(String key) {
        data.remove(key);
    }

    @Override
    public void close() throws IOException {
        // In-memory implementation doesn't require any specific close logic
    }
}
