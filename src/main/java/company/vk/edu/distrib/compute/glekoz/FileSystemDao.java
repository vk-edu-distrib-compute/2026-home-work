package company.vk.edu.distrib.compute.glekoz;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.nio.charset.StandardCharsets.UTF_8;

public class FileSystemDao implements Dao<byte[]> {
    private static final String HASH_ALGORITHM = "SHA-256";
    private static final int DIR_PREFIX_LENGTH = 2;

    private final Path dataDir;
    private final ConcurrentMap<String, ReadWriteLock> locks;

    public FileSystemDao(Path dataDir) throws IOException {
        this.dataDir = dataDir;
        this.locks = new ConcurrentHashMap<>();
        Files.createDirectories(dataDir);
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        validateKey(key);
        ReadWriteLock lock = getLock(key);
        lock.readLock().lock();
        try {
            Path file = keyToPath(key);
            if (!Files.exists(file)) {
                throw new NoSuchElementException("No value for key: " + key);
            }
            return Files.readAllBytes(file);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        validateKey(key);
        validateValue(value);
        ReadWriteLock lock = getLock(key);
        lock.writeLock().lock();
        try {
            Path file = keyToPath(key);
            Files.createDirectories(file.getParent());
            Path tmpFile = file.resolveSibling(file.getFileName().toString() + ".tmp");
            Files.write(tmpFile, value);
            Files.move(tmpFile, file, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        validateKey(key);
        ReadWriteLock lock = getLock(key);
        lock.writeLock().lock();
        try {
            Files.deleteIfExists(keyToPath(key));
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        // no-op
    }

    private Path keyToPath(String key) {
        String hex = hashKey(key);
        return dataDir
                .resolve(hex.substring(0, DIR_PREFIX_LENGTH))
                .resolve(hex.substring(DIR_PREFIX_LENGTH));
    }

    private static String hashKey(String key) {
        try {
            MessageDigest digest = MessageDigest.getInstance(HASH_ALGORITHM);
            byte[] hash = digest.digest(key.getBytes(UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException(HASH_ALGORITHM + " unavailable", ex);
        }
    }

    private ReadWriteLock getLock(String key) {
        return locks.computeIfAbsent(key, k -> new ReentrantReadWriteLock());
    }

    private static void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be null or empty");
        }
    }

    private static void validateValue(byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException("Value must not be null");
        }
    }
}
