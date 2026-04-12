package company.vk.edu.distrib.compute.technodamo;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Base64;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileDao implements Dao<byte[]> {
    private static final String TEMP_SUFFIX = ".tmp";

    private final Path root;
    private final ConcurrentMap<String, ReentrantReadWriteLock> locks = new ConcurrentHashMap<>();

    public FileDao(Path root) throws IOException {
        this.root = root;
        Files.createDirectories(root);
    }

    @Override
    public byte[] get(String key) throws IOException {
        validateKey(key);
        ReentrantReadWriteLock lock = lock(key);
        lock.readLock().lock();
        try {
            Path file = filePath(key);
            if (!Files.exists(file)) {
                throw new NoSuchElementException(key);
            }
            return Files.readAllBytes(file);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        validateKey(key);
        validateValue(value);
        ReentrantReadWriteLock lock = lock(key);
        lock.writeLock().lock();
        try {
            Path file = filePath(key);
            Path tempFile = file.resolveSibling(file.getFileName() + TEMP_SUFFIX);
            Files.write(tempFile, value);
            moveAtomically(tempFile, file);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void delete(String key) throws IOException {
        validateKey(key);
        ReentrantReadWriteLock lock = lock(key);
        lock.writeLock().lock();
        try {
            Files.deleteIfExists(filePath(key));
            locks.remove(key, lock);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        locks.clear();
    }

    private Path filePath(String key) {
        byte[] encoded = Base64.getUrlEncoder().withoutPadding().encode(key.getBytes(StandardCharsets.UTF_8));
        return root.resolve(new String(encoded, StandardCharsets.US_ASCII));
    }

    private ReentrantReadWriteLock lock(String key) {
        return locks.computeIfAbsent(key, ignored -> new ReentrantReadWriteLock());
    }

    private static void moveAtomically(Path source, Path target) throws IOException {
        try {
            Files.move(
                    source,
                    target,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING
            );
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be null or empty");
        }
    }

    private static void validateValue(byte[] value) {
        if (Objects.isNull(value)) {
            throw new IllegalArgumentException("Value must not be null");
        }
    }
}
