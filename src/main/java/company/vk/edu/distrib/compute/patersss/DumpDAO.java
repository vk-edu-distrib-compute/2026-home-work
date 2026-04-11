package company.vk.edu.distrib.compute.patersss;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class DumpDAO implements Dao<byte[]> {
    private final Map<String, byte[]> repository = new ConcurrentHashMap<>();
    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        keyCheck(key);
        final var value = repository.get(key);

        if (value == null) {
            throw new NoSuchElementException("There is no element with key " + key);
        }

        return value;
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        keyCheck(key);
        repository.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        keyCheck(key);
        repository.remove(key);
    }

    @Override
    public void close() throws IOException {
        System.out.println("Map was exactly saved");
    }

    private void keyCheck(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Bad key. Null or empty");
        }
    }
}
