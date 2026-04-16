package company.vk.edu.distrib.compute.teeaamma;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class TeeaammaDao implements Dao<byte[]> {

    private final Map<String, byte[]> storage = new ConcurrentHashMap<>();

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key is null or blank");
        }
        final var value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException("no value for key " + key);
        }
        return value;
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key is null or blank");
        }
        storage.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key is null or blank");
        }
        storage.remove(key);
    }

    @Override
    public void close() throws IOException {
        // Сохранение в памяти, никакие ресурсы закрывать не надо
    }
}
