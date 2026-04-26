package company.vk.edu.distrib.compute.denchika.dao;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileSystemDao extends DaoBase implements Dao<byte[]> {

    private final Path baseDir;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public FileSystemDao(Path baseDir) throws IOException {
        super();
        this.baseDir = baseDir;
        Files.createDirectories(baseDir);
    }

    @Override
    public byte[] get(String key) throws IOException {
        validateKey(key);

        lock.readLock().lock();
        try {
            Path file = baseDir.resolve(encode(key));
            try {
                return Files.readAllBytes(file);
            } catch (NoSuchFileException e) {
                throw new NoSuchElementException("Key not found", e);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        validateKey(key);

        if (value == null || value.length > MAX_SIZE) {
            throw new IllegalArgumentException("Invalid value size");
        }

        lock.writeLock().lock();
        try {
            Path file = baseDir.resolve(encode(key));
            Files.write(file, value,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void delete(String key) throws IOException {
        validateKey(key);

        lock.writeLock().lock();
        try {
            Path file = baseDir.resolve(encode(key));
            Files.deleteIfExists(file);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        // nothing to close
    }

    private static String encode(String key) {
        return Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(key.getBytes(StandardCharsets.UTF_8));
    }
}
