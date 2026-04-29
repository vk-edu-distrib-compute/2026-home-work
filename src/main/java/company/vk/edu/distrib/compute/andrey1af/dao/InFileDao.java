package company.vk.edu.distrib.compute.andrey1af.dao;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class InFileDao implements Dao<byte[]> {

    private static final String FILE_EXTENSION = ".bin";
    private static final int MAX_KEY_LENGTH = 1024;

    private final Path directory;
    private final Map<String, byte[]> cache;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public InFileDao(Path directory) {
        this.directory = directory;
        this.cache = new ConcurrentHashMap<>();

        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot initialize storage directory: " + directory, e);
        }
    }

    @Override
    public byte[] get(String key) throws IOException {
        checkNotClosed();
        checkKey(key);

        byte[] cached = cache.get(key);
        if (cached != null) {
            return Arrays.copyOf(cached, cached.length);
        }

        Path file = fileForKey(key);
        try {
            byte[] value = Files.readAllBytes(file);
            cache.put(key, value);
            return Arrays.copyOf(value, value.length);
        } catch (NoSuchFileException e) {
            NoSuchElementException exception = new NoSuchElementException("Key not found: " + key);
            exception.initCause(e);
            throw exception;
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        checkNotClosed();
        checkKey(key);

        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }

        byte[] storedValue = Arrays.copyOf(value, value.length);
        Path target = fileForKey(key);
        Path tempFile = Files.createTempFile(directory, "infile-dao-", ".tmp");

        try {
            Files.write(tempFile, storedValue);
            moveAtomicallyOrReplace(tempFile, target);
            cache.put(key, storedValue);
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    @Override
    public void delete(String key) throws IOException {
        checkNotClosed();
        checkKey(key);

        Files.deleteIfExists(fileForKey(key));
        cache.remove(key);
    }

    @Override
    public void close() {
        closed.set(true);
        cache.clear();
    }

    private void checkKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key cannot be null or blank");
        }
        if (key.length() > MAX_KEY_LENGTH) {
            throw new IllegalArgumentException("Key is too long");
        }
    }

    private void checkNotClosed() {
        if (closed.get()) {
            throw new IllegalStateException("DAO is closed");
        }
    }

    private Path fileForKey(String key) {
        String encoded = Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(key.getBytes(StandardCharsets.UTF_8));
        return directory.resolve(encoded + FILE_EXTENSION);
    }

    private void moveAtomicallyOrReplace(Path source, Path target) throws IOException {
        try {
            Files.move(source, target,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
