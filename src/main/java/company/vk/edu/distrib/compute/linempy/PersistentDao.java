package company.vk.edu.distrib.compute.linempy;

import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Persistent Dao — хранилище данных в файловой системе.
 *
 * <p>Каждый ключ сохраняется в отдельный файл в директории данных.
 * Использует блокировки для потокобезопасности.</p>
 *
 * @author Linempy
 * @since 29.03.2026
 */
public class PersistentDao implements Dao<byte[]> {

    private static final Logger log = LoggerFactory.getLogger(PersistentDao.class);

    private final Path dataDir;
    private final ConcurrentMap<String, ReentrantReadWriteLock> locks = new ConcurrentHashMap<>();

    /**
     * Создает PersistentDao с указанной директорией для хранения данных.
     *
     * @param dataDirPath путь к директории для хранения файлов
     * @throws IOException если не удается создать директорию
     */
    public PersistentDao(String dataDirPath) throws IOException {
        this.dataDir = Paths.get(dataDirPath);
        if (!Files.exists(dataDir)) {
            Files.createDirectories(dataDir);
        }
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        validateKey(key);

        ReentrantReadWriteLock lock = getLock(key);
        lock.readLock().lock();
        try {
            Path filePath = getFilePath(key);
            if (!Files.exists(filePath)) {
                throw new NoSuchElementException("Value not found for key: " + key);
            }
            return Files.readAllBytes(filePath);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        validateKey(key);
        validateValue(value);

        ReentrantReadWriteLock lock = getLock(key);
        lock.writeLock().lock();
        try {
            Path filePath = getFilePath(key);
            Files.write(filePath, value, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        validateKey(key);

        ReentrantReadWriteLock lock = getLock(key);
        lock.writeLock().lock();
        try {
            Path filePath = getFilePath(key);
            Files.deleteIfExists(filePath);
            locks.remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        log.info("Closing PersistentDao, clearing locks");
        locks.clear();
    }

    /**
     * Получает путь к файлу для ключа.
     * Заменяет недопустимые символы в имени файла.
     */
    private Path getFilePath(String key) {
        String safeKey = key.replaceAll("[^a-zA-Z0-9.-]", "_");
        return dataDir.resolve(safeKey + ".dat");
    }

    private ReentrantReadWriteLock getLock(String key) {
        return locks.computeIfAbsent(key, k -> new ReentrantReadWriteLock());
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
}
