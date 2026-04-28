package company.vk.edu.distrib.compute.mediocritas.storage;

import company.vk.edu.distrib.compute.Dao;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import static company.vk.edu.distrib.compute.mediocritas.storage.validation.DaoKeyValidatorUtils.validateKey;

public class InMemoryByteDao implements Dao<byte[]> {

    private final Map<String, byte[]> storage;

    public InMemoryByteDao() {
        this.storage = new ConcurrentHashMap<>();
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException {
        byte[] value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException("No element with id: " + key);
        }
        return value;
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException {
        validateKey(key);
        storage.put(key, value);

    }

    @Override
    public void delete(String key) throws IllegalArgumentException {
        validateKey(key);
        storage.remove(key);
    }

    @Override
    public void close() {
        storage.clear();
    }
}
