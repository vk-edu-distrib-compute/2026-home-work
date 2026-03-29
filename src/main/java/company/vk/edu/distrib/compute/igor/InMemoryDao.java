package company.vk.edu.distrib.compute.igor;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryDao implements Dao<byte[]> {
    private final ConcurrentMap<String, byte[]> storage = new ConcurrentHashMap<>();

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        byte[] value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException("No value for key: " + key);
        }

        return Arrays.copyOf(value, value.length);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        storage.put(key, Arrays.copyOf(value, value.length));
    }

    @Override
    public void delete(String key) throws IllegalArgumentException {
        storage.remove(key);
    }

    @Override
    public void close(){
    }
}
