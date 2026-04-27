package company.vk.edu.distrib.compute.marinchanka;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class VersionedInMemoryDao implements Dao<byte[]> {
    private final Map<String, VersionedEntry> storage = new ConcurrentHashMap<>();
    private boolean closed;

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        checkClosed();
        validateKey(key);

        VersionedEntry entry = storage.get(key);
        if (entry == null || entry.tombstone) {
            throw new NoSuchElementException("Key not found: " + key);
        }
        return entry.data.clone();
    }

    /**
     * Возвращает версию ключа. Если ключ не найден, возвращает -1.
     */
    public long getVersion(String key) throws IOException {
        checkClosed();
        VersionedEntry entry = storage.get(key);
        return (entry != null) ? entry.version : -1;
    }

    /**
     * Возвращает данные вместе с версией.
     */
    public VersionedEntry getEntry(String key) throws IOException {
        checkClosed();
        return storage.get(key);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        checkClosed();
        validateKey(key);
        validateValue(value);

        // Атомарное обновление с увеличением версии
        storage.compute(key, (k, existing) -> {
            long newVersion = (existing == null) ? 1L : existing.version + 1;
            return new VersionedEntry(value.clone(), newVersion, false);
        });
    }

    /**
     * Запись с указанной версией (используется для repair синхронизации).
     */
    public void upsertWithVersion(String key, byte[] value, long version) throws IOException {
        checkClosed();
        validateKey(key);
        validateValue(value);

        storage.compute(key, (k, existing) -> {
            if (existing == null || version >= existing.version) {
                return new VersionedEntry(value.clone(), version, false);
            }
            return existing;
        });
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        checkClosed();
        validateKey(key);

        // Удаление – записываем tombstone с увеличенной версией
        storage.compute(key, (k, existing) -> {
            long newVersion = (existing == null) ? 1L : existing.version + 1;
            return new VersionedEntry(null, newVersion, true);
        });
    }

    /**
     * Удаление с указанной версией (для repair синхронизации).
     */
    public void deleteWithVersion(String key, long version) throws IOException {
        checkClosed();
        validateKey(key);

        storage.compute(key, (k, existing) -> {
            if (existing == null || version >= existing.version) {
                return new VersionedEntry(null, version, true);
            }
            return existing;
        });
    }

    @Override
    public void close() throws IOException {
        closed = true;
        storage.clear();
    }

    private void validateKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
    }

    private void validateValue(byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
    }

    private void checkClosed() throws IOException {
        if (closed) {
            throw new IOException("DAO is closed");
        }
    }

    public static class VersionedEntry {
        public final byte[] data;
        public final long version;
        public final boolean tombstone;

        public VersionedEntry(byte[] data, long version, boolean tombstone) {
            this.data = data != null ? data.clone() : null;
            this.version = version;
            this.tombstone = tombstone;
        }
    }
}
