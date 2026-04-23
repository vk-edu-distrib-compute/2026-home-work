package company.vk.edu.distrib.compute.semenmartynov;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory implementation of the {@link Dao} interface using a thread-safe {@link ConcurrentHashMap}.
 */
public class InMemoryDao implements Dao<byte[]> {
    private final Map<String, byte[]> storage = new ConcurrentHashMap<>();

    /**
     * Retrieves the value associated with the given key.
     *
     * @param key the unique identifier
     * @return the byte array associated with the key
     * @throws NoSuchElementException   if the key is not found in the storage
     * @throws IllegalArgumentException if the key is null or empty
     */
    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        byte[] value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException("Key not found: " + key);
        }
        return value;
    }

    /**
     * Creates or updates the value for the given key.
     *
     * @param key   the unique identifier
     * @param value the data to store
     * @throws IllegalArgumentException if the key is null or empty
     */
    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        storage.put(key, value);
    }

    /**
     * Deletes the value associated with the given key if it exists.
     *
     * @param key the unique identifier
     * @throws IllegalArgumentException if the key is null or empty
     */
    @Override
    public void delete(String key) throws IllegalArgumentException {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        storage.remove(key);
    }

    /**
     * Closes the DAO. In this in-memory implementation, it clears the storage.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        storage.clear();
    }
}
