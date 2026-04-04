package company.vk.edu.distrib.compute.wedwincode;

import company.vk.edu.distrib.compute.Dao;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryDao<T> implements Dao<T> {
    private final Map<String, T> storage;

    public InMemoryDao() {
        storage = new ConcurrentHashMap<>();
    }

    @Override
    public T get(String key) throws NoSuchElementException, IllegalArgumentException {
        checkKey(key);
        T value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException("element with key '" + key + "' not found");
        }

        return value;
    }

    @Override
    public void upsert(String key, T value) throws IllegalArgumentException {
        checkKey(key);
        if (value == null) {
            throw new IllegalArgumentException("value must not be null");
        }
        storage.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException {
        checkKey(key);
        storage.remove(key);
    }

    @Override
    public void close() {
        // not needed in InMemoryDao
    }

    private void checkKey(String key) throws IllegalArgumentException {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key must not be null or empty");
        }
    }
}
