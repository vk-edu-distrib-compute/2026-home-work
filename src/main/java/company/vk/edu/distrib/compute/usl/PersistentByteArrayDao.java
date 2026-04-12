package company.vk.edu.distrib.compute.usl;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Base64;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PersistentByteArrayDao implements Dao<byte[]> {
    private final Path rootDirectory;
    private final ConcurrentMap<String, ReentrantReadWriteLock> locks = new ConcurrentHashMap<>();

    public PersistentByteArrayDao(Path rootDirectory) throws IOException {
        this.rootDirectory = rootDirectory;
        Files.createDirectories(rootDirectory);
        if (!Files.isDirectory(rootDirectory)) {
            throw new IllegalArgumentException("Storage path is not a directory: " + rootDirectory);
        }
    }

    @Override
    public byte[] get(String key) throws IOException {
        validateKey(key);
        Path file = filePath(key);
        ReentrantReadWriteLock lock = lock(key);

        lock.readLock().lock();
        try {
            if (!Files.exists(file)) {
                throw new NoSuchElementException("No value for key: " + key);
            }
            return Files.readAllBytes(file);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        validateKey(key);
        if (value == null) {
            throw new IllegalArgumentException("Value must not be null");
        }

        Path file = filePath(key);
        Path temporaryFile = temporaryFile(file);
        ReentrantReadWriteLock lock = lock(key);

        lock.writeLock().lock();
        try {
            Files.write(temporaryFile, value);
            Files.move(
                temporaryFile,
                file,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
            );
        } finally {
            Files.deleteIfExists(temporaryFile);
            lock.writeLock().unlock();
        }
    }

    @Override
    public void delete(String key) throws IOException {
        validateKey(key);
        Path file = filePath(key);
        ReentrantReadWriteLock lock = lock(key);

        lock.writeLock().lock();
        try {
            Files.deleteIfExists(file);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        // The storage is persisted on disk, nothing to release.
    }

    private Path filePath(String key) {
        return rootDirectory.resolve(encodedKey(key) + ".bin");
    }

    private static Path temporaryFile(Path file) {
        return file.resolveSibling(
            file.getFileName() + ".tmp-" + Long.toUnsignedString(ThreadLocalRandom.current().nextLong())
        );
    }

    private ReentrantReadWriteLock lock(String key) {
        return locks.computeIfAbsent(key, ignored -> new ReentrantReadWriteLock());
    }

    private static String encodedKey(String key) {
        return Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(key.getBytes(StandardCharsets.UTF_8));
    }

    private static void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be empty");
        }
    }
}
