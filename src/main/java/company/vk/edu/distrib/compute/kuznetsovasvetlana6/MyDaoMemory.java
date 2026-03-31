package company.vk.edu.distrib.compute.kuznetsovasvetlana6;

import company.vk.edu.distrib.compute.Dao;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.NoSuchElementException;

public class MyDaoMemory implements Dao<byte[]> {
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
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be empty");
        }
        storage.put(key, value);
    }

    @Override
    public void delete(String key) {
        storage.remove(key);
    }

    @Override
    public void close() throws IOException {
        storage.clear();
    }
}
