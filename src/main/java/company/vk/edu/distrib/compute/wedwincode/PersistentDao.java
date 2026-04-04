package company.vk.edu.distrib.compute.wedwincode;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.NoSuchElementException;

public class PersistentDao implements Dao<byte[]> {
    private final Path storageDir;

    public PersistentDao(Path storageDir) throws IOException {
        this.storageDir = storageDir;
        Files.createDirectories(storageDir);
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        checkKey(key);
        Path file = resolvePath(key);
        if (!Files.exists(file)) {
            throw new NoSuchElementException("key not found: " + key);
        }

        return Files.readAllBytes(file);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        checkKey(key);
        if (value == null) {
            throw new IllegalArgumentException("value must not be null");
        }

        Path file = resolvePath(key);
        Files.write(
                file,
                value,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
        );
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        checkKey(key);
        Path file = resolvePath(key);
        Files.deleteIfExists(file);
    }

    @Override
    public void close() {
        // not needed in PersistentDao
    }

    private Path resolvePath(String key) {
        String fileName = Base64.getUrlEncoder().withoutPadding().encodeToString(key.getBytes());
        return storageDir.resolve(fileName);
    }

    private void checkKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key must not be null or empty");
        }
    }
}
