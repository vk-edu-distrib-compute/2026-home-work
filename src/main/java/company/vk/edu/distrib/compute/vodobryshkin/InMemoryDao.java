package company.vk.edu.distrib.compute.vodobryshkin;

import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Dao в оперативной памяти.
 *
 * <p>
 *     Сохраняет всю информацию в ConcurrentHashMap.
 * </p>
 */
public class InMemoryDao implements Dao<byte[]> {
    private final Map<String, byte[]> storageDict = new ConcurrentHashMap<>();
    private static final Logger log = LoggerFactory.getLogger("server");

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException {
        byte[] value = storageDict.get(key);

        if (value == null) {
            throw new NoSuchElementException();
        }

        return value;
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException {
        storageDict.put(key, value);
        log.debug("Successfully added value of length={} under key={}", value.length, key);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException {
        storageDict.remove(key);
        log.debug("Successfully removed value under key={}", key);
    }

    @Override
    public void close() {
        // no implementation
    }
}
