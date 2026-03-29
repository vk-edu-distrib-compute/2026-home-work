package company.vk.edu.distrib.compute.Lillymega;
import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class LillymegaDao implements Dao<byte[]> {
    private final Map<String, byte[]> storage = new ConcurrentHashMap<>();

    @Override
    public byte[] get(String key){
        validateKey (key);
        byte[] value = storage.get(key);
        if (value == null){
            throw new NoSuchElementException("No value for key: " + key);
        }
    }

    @Override
    public void upsert(String key, byte[] value) {
        validateKey (key);
        if (value == null) {
            throw new IllegalArgumentException("Value must be not null");
        }
    }

    @Override
    public void delete(String key) {
        validateKey (key);
        storage.remove(key);
    }
    @Override
    public void close() {
        storage.clear();
    }
    private static void validateKey(String key){
        if (key == null || key.isEmpty()){
            throw new IllegalArgumentException("Key must be not empty");
        }
    }
}