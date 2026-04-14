package company.vk.edu.distrib.compute.t1d333;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class FileDao implements Dao<byte[]> {
    private final Path rootDir;
    private final Lock lock = new ReentrantLock();

    public FileDao(Path rootDir) throws IOException {
        this.rootDir = rootDir;
        Files.createDirectories(rootDir);
    }

    public FileDao(String rootDir) throws IOException {
        this(Path.of(rootDir));
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        lock.lock();
        try {
            Path file = fileFor(key);
            if (Files.notExists(file)) {
                throw new NoSuchElementException("Key not found: " + key);
            }
            return Files.readAllBytes(file);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        lock.lock();
        try {
            Files.write(
                fileFor(key),
                value,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
            );
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        lock.lock();
        try {
            Files.deleteIfExists(fileFor(key));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        // persistent filesystem storage: nothing to close
    }

    private Path fileFor(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }

        String encodedKey = Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(key.getBytes(StandardCharsets.UTF_8));
        return rootDir.resolve(encodedKey + ".dat");
    }
}
