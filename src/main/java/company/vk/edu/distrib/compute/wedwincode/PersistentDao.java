package company.vk.edu.distrib.compute.wedwincode;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Base64;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

public class PersistentDao implements Dao<byte[]> {
    private final Path storageDir;
    private final ConcurrentMap<String, ReentrantLock> keyLocks;

    public PersistentDao(Path storageDir) throws IOException {
        this.storageDir = storageDir;
        this.keyLocks = new ConcurrentHashMap<>();
        Files.createDirectories(storageDir);
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        checkKey(key);
        ReentrantLock lock = keyLocks.computeIfAbsent(key, k -> new ReentrantLock());
        lock.lock();
        try {
            Path file = resolvePath(key);
            if (!Files.exists(file)) {
                throw new NoSuchElementException("key not found: " + key);
            }

            return Files.readAllBytes(file);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        checkKey(key);
        if (value == null) {
            throw new IllegalArgumentException("value must not be null");
        }

        ReentrantLock lock = keyLocks.computeIfAbsent(key, k -> new ReentrantLock());
        lock.lock();
        try {
            Path file = resolvePath(key);
            Path tempFile = Files.createTempFile(storageDir, "tmp-", ".bin");

            try {
                Files.write(tempFile, value);

                try {
                    Files.move(tempFile, file,
                            StandardCopyOption.REPLACE_EXISTING,
                            StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(tempFile, file,
                            StandardCopyOption.REPLACE_EXISTING);
                }
            } finally {
                Files.deleteIfExists(tempFile);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        checkKey(key);
        ReentrantLock lock = keyLocks.computeIfAbsent(key, k -> new ReentrantLock());
        lock.lock();
        try {
            Path file = resolvePath(key);
            Files.deleteIfExists(file);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        // not needed in PersistentDao
    }

    private Path resolvePath(String key) {
        String fileName = Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(key.getBytes(StandardCharsets.UTF_8));
        return storageDir.resolve(fileName);
    }

    private void checkKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key must not be null or empty");
        }
    }
}
