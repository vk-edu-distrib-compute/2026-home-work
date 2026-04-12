package company.vk.edu.distrib.compute.dariaprindina;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Base64;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;

public class DPFileDao implements Dao<byte[]> {
    private static final Base64.Encoder BASE64_URL = Base64.getUrlEncoder().withoutPadding();

    private final Path rootDir;
    private final ReentrantLock lock = new ReentrantLock();

    public DPFileDao(Path rootDir) throws IOException {
        this.rootDir = rootDir;
        Files.createDirectories(rootDir);
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        validateKey(key);
        lock.lock();
        try {
            final Path keyPath = pathForKey(key);
            if (!Files.exists(keyPath)) {
                throw new NoSuchElementException("no value for key " + key);
            }
            return Files.readAllBytes(keyPath);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        validateKey(key);
        if (value == null) {
            throw new IllegalArgumentException("value is null");
        }
        lock.lock();
        try {
            final Path keyPath = pathForKey(key);
            final Path tmpPath = rootDir.resolve(
                "tmp-" + ThreadLocalRandom.current().nextInt(1_000_000) + ".bin"
            );
            Files.write(tmpPath, value);
            Files.move(tmpPath, keyPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        validateKey(key);
        lock.lock();
        try {
            Files.deleteIfExists(pathForKey(key));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        // no resources
    }

    private Path pathForKey(String key) {
        final String safeFileName = BASE64_URL.encodeToString(key.getBytes(StandardCharsets.UTF_8));
        return rootDir.resolve(safeFileName + ".bin");
    }

    private static void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or blank");
        }
    }
}
