package company.vk.edu.distrib.compute.marinchanka;

import company.vk.edu.distrib.compute.Dao;
import java.io.IOException;
import java.nio.file.*;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PersistentDao implements Dao<byte[]> {
    private final Path storageDir;
    private final ConcurrentHashMap<String, ReadWriteLock> keyLocks = new ConcurrentHashMap<>();
    private volatile boolean closed = false;

    public PersistentDao(String storagePath) throws IOException {
        this.storageDir = Paths.get(storagePath);
        if (!Files.exists(storageDir)) {
            Files.createDirectories(storageDir);
        }
        if (!Files.isDirectory(storageDir)) {
            throw new IOException("Path exists but is not a directory: " + storagePath);
        }
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        checkClosed();
        validateKey(key);

        ReadWriteLock lock = getKeyLock(key);
        lock.readLock().lock();
        try {
            Path filePath = getFilePath(key);
            if (!Files.exists(filePath)) {
                throw new NoSuchElementException("Key not found: " + key);
            }
            return Files.readAllBytes(filePath);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        checkClosed();
        validateKey(key);
        validateValue(value);

        ReadWriteLock lock = getKeyLock(key);
        lock.writeLock().lock();
        try {
            Path filePath = getFilePath(key);
            Path tempFile = Files.createTempFile(storageDir, "tmp_" + key + "_", ".dat");
            try {
                Files.write(tempFile, value);
                Files.move(tempFile, filePath, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                Files.deleteIfExists(tempFile);
                throw e;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        checkClosed();
        validateKey(key);

        ReadWriteLock lock = getKeyLock(key);
        lock.writeLock().lock();
        try {
            Path filePath = getFilePath(key);
            Files.deleteIfExists(filePath);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        closed = true;
    }

    private Path getFilePath(String key) {
        String safeKey = key.replaceAll("[^a-zA-Z0-9.-]", "_");
        return storageDir.resolve(safeKey + ".dat");
    }

    private ReadWriteLock getKeyLock(String key) {
        return keyLocks.computeIfAbsent(key, k -> new ReentrantReadWriteLock());
    }

    private void validateKey(String key) {
        if (key == null || key.trim().isEmpty()) {
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
}