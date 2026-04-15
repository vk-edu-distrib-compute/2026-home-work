package company.vk.edu.distrib.compute.kruchinina;

import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory имплементация Dao, не используется из-за перехода на FileSystemDao.
 */
public class InMemoryDao implements Dao<byte[]> {
    private static final Logger LOG = LoggerFactory.getLogger(InMemoryDao.class);
    //Хранилище данных: ConcurrentHashMap обеспечивает потокобезопасность
    private final Map<String, byte[]> storage = new ConcurrentHashMap<>();

    //Возвращает значение по ключу, может выбросить исключения
    @Override
    public byte[] get(final String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        validateKey(key); //Вызов приватного метода для проверки ключа (не null и не пустой)
        final byte[] value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException("No value for key: " + key);
        }
        return value.clone(); //Для защиты от влияния извне
    }

    @Override
    public void upsert(final String key, final byte[] value) throws IllegalArgumentException, IOException {
        validateKey(key);
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        storage.put(key, value.clone());
    }

    @Override
    public void delete(final String key) throws IllegalArgumentException, IOException {
        validateKey(key);
        storage.remove(key);
    }

    @Override
    public void close() {
        storage.clear();
        LOG.debug("InMemoryDao closed");
    }

    private static void validateKey(final String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must be non-empty");
        }
    }
}
