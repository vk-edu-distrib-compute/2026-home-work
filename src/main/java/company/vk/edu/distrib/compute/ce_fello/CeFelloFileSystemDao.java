package company.vk.edu.distrib.compute.ce_fello;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Base64;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CeFelloFileSystemDao implements Dao<byte[]> {
    private static final String TEMP_SUFFIX = ".tmp";

    private final Path rootDirectory;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicBoolean closed = new AtomicBoolean();

    public CeFelloFileSystemDao(Path rootDirectory) throws IOException {
        this.rootDirectory = Objects.requireNonNull(rootDirectory, "rootDirectory");
        Files.createDirectories(rootDirectory);
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        validateKey(key);
        lock.readLock().lock();
        try {
            ensureOpen();
            return Files.readAllBytes(filePath(key));
        } catch (NoSuchFileException e) {
            NoSuchElementException exception = new NoSuchElementException("Key not found: " + key);
            exception.initCause(e);
            throw exception;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        validateKey(key);
        if (value == null) {
            throw new IllegalArgumentException("value must not be null");
        }
        lock.writeLock().lock();
        try {
            ensureOpen();
            Path targetPath = filePath(key);
            Path tempPath = rootDirectory.resolve(targetPath.getFileName() + TEMP_SUFFIX);
            Files.write(tempPath, value);
            moveReplacing(tempPath, targetPath);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        validateKey(key);
        lock.writeLock().lock();
        try {
            ensureOpen();
            Files.deleteIfExists(filePath(key));
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        closed.set(true);
    }

    private Path filePath(String key) {
        return rootDirectory.resolve(encodeKey(key));
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("DAO is already closed");
        }
    }

    private static void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key must be non-empty");
        }
    }

    private static String encodeKey(String key) {
        return Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(key.getBytes(StandardCharsets.UTF_8));
    }

    private static void moveReplacing(Path sourcePath, Path targetPath) throws IOException {
        try {
            Files.move(sourcePath, targetPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
