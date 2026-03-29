package company.vk.edu.distrib.compute.vodobryshkin.file;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.vodobryshkin.InMemoryDao;

import java.io.IOException;
import java.util.*;

public class CacheDao implements Dao<byte[]> {
    private final int limit;
    private final Dao<byte[]> dao;
    private final Deque<String> cachedKeys = new ArrayDeque<>();

    public CacheDao(int limit) {
        this(limit, new InMemoryDao());
    }

    public CacheDao(int limit, Dao<byte[]> dao) {
        if (limit <= 0) {
            throw new IllegalArgumentException("Cache must contain more than 0 elements.");
        }

        this.limit = limit;
        this.dao = dao;
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        return dao.get(key);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        boolean present;

        try {
            dao.get(key);
            present = true;
        } catch (NoSuchElementException e) {
            present = false;
        }

        if (present) {
            cachedKeys.remove(key);
        } else if (cachedKeys.size() == limit) {
            String firstKey = cachedKeys.removeFirst();
            dao.delete(firstKey);
        }

        cachedKeys.addLast(key);
        dao.upsert(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        dao.delete(key);
        cachedKeys.remove(key);
    }

    @Override
    public void close() throws IOException {
        dao.close();
    }
}
