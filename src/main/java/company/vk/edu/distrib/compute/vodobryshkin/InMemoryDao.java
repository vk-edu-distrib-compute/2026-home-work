package company.vk.edu.distrib.compute.vodobryshkin;

import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryDao implements Dao<byte[]> {
    private final Map<String, byte[]> storageDict = new ConcurrentHashMap<>();
    private static final Logger log = LoggerFactory.getLogger("server");

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        return storageDict.get(key);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        storageDict.put(key, value);
        log.debug("Successfully added value={} under key={}", value, key);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        storageDict.remove(key);
        log.debug("Successfully removed value under key={}", key);
    }

    @Override
    public void close() throws IOException {
        // no implementation
    }
}
