package company.vk.edu.distrib.compute.linempy;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * DaoImpl — описание класса.
 *
 * <p>
 *
 * @author Linempy
 * @since 28.03.2026
 */
public class DaoImpl<T> implements Dao<T> {
    private final ConcurrentMap<String, T> storage;

    public DaoImpl() {
        this.storage = new ConcurrentHashMap<>();
    }

    @Override
    public T get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        validateKey(key);

        T value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException(String.format("Значение не было найдено для ключа: '%s'", key));
        }
        return value;
    }

    @Override
    public void upsert(String key, T value) throws IllegalArgumentException, IOException {
        validateKey(key);

        if (value == null) {
            throw new IllegalArgumentException("Значение не может быть null");
        }
        storage.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        validateKey(key);
        storage.remove(key);
    }

    @Override
    public void close() throws IOException {
        //nothing
    }

    private void validateKey(String key) {
        if (key.isBlank()) {
            throw new IllegalArgumentException("Ключ не может быть null или пустым");
        }
    }
}
