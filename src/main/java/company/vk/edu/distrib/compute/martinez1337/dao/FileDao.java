package company.vk.edu.distrib.compute.martinez1337.dao;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.NoSuchElementException;

public class FileDao implements Dao<byte[]> {
    private final Path baseDir;

    public FileDao(Path baseDir) throws IOException {
        this.baseDir = baseDir;
        Files.createDirectories(baseDir);
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        Path path = resolvePath(key);
        if (!Files.exists(path)) {
            throw new NoSuchElementException("no value for key: " + key);
        }
        return Files.readAllBytes(path);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        Path path = resolvePath(key);
        Files.write(path, value, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        Path path = resolvePath(key);
        Files.deleteIfExists(path);
    }

    @Override
    public void close() throws IOException {
        // no resource needs to be closed
    }

    private Path resolvePath(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key cannot be empty");
        }
        String safeKey = Base64.getUrlEncoder().encodeToString(key.getBytes());
        return baseDir.resolve(safeKey);
    }
}
