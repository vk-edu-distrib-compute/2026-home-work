package company.vk.edu.distrib.compute.denchika.dao;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryDao extends DaoBase implements Dao<byte[]> {

    private final Map<String, byte[]> data = new ConcurrentHashMap<>();

    @Override
    public byte[] get(String key) throws IOException {
        validateKey(key);
        byte[] value = data.get(key);
        if (value == null) {
            throw new NoSuchElementException("Not found: " + key);
        }
        return value;
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        validateKey(key);
        if (value == null || value.length > MAX_SIZE) {
            throw new IllegalArgumentException("Invalid value");
        }
        data.put(key, value);
    }

    @Override
    public void delete(String key) throws IOException {
        validateKey(key);
        data.remove(key);
    }

    @Override
    public void close() {
        // nothing
    }
}
