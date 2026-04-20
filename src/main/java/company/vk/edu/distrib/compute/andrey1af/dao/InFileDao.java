package company.vk.edu.distrib.compute.andrey1af.dao;

import company.vk.edu.distrib.compute.Dao;

import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class InFileDao implements Dao<byte[]> {

    private final Path directory;
    private final Map<String, byte[]> cache;

    public InFileDao(Path filePath) {
        this.directory = filePath;
        this.cache = new ConcurrentHashMap<>();
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot initialize storage directory: " + directory, e);
        }
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        checkKey(key);

        byte[] cached = cache.get(key);
        if (cached != null) {
            return Arrays.copyOf(cached, cached.length);
        }

        Path file = fileForKey(key);
        if (!Files.exists(file)) {
            throw new NoSuchElementException("Key not found: " + key);
        }
        byte[] value = Files.readAllBytes(file);
        cache.put(key, value);
        return Arrays.copyOf(value, value.length);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        checkKey(key);
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        byte[] storedValue = Arrays.copyOf(value, value.length);

        Path target = fileForKey(key);
        Path tempFile = Files.createTempFile(directory, "infile-dao-", ".tmp");
        try {
            Files.write(tempFile, storedValue);
            try {
                Files.move(tempFile, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException ignored) {
                Files.move(tempFile, target, StandardCopyOption.REPLACE_EXISTING);
            }
            cache.put(key, storedValue);
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        checkKey(key);
        cache.remove(key);
        Files.deleteIfExists(fileForKey(key));
    }

    @Override
    public void close() throws IOException {
        cache.clear();
    }

    private void checkKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key cannot be null or blank");
        }
    }

    private Path fileForKey(String key) {
        String encoded = Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(key.getBytes(StandardCharsets.UTF_8));
        return directory.resolve(encoded + ".bin");
    }
}
