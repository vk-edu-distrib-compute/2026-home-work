package company.vk.edu.distrib.compute.maryarta;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryDao implements Dao<byte[]> {
    private final Map<String, byte[]> storage = new ConcurrentHashMap<>();

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        if (key.isBlank()) {
            throw new IllegalArgumentException("Key is blank");
        }
        final byte [] value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException("No element for key " + key);
        }
        return value;
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        if (key.isBlank()) {
            throw new IllegalArgumentException("Key is blank");
        }
        storage.put(key,value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        if (key.isBlank()) {
            throw new IllegalArgumentException("Key is blank");
        }
        storage.remove(key);
    }

    @Override
    public void close() throws IOException {
        //no-op
    }
}
