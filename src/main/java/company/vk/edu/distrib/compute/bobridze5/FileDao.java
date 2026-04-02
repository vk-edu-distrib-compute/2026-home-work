package company.vk.edu.distrib.compute.bobridze5;

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
        if (!Files.exists(baseDir)) {
            Files.createDirectories(baseDir);
        }
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        Path filePath = getPath(key);
        if (!Files.exists(filePath)) {
            throw new NoSuchElementException("Key not found: " + key);
        }

        return Files.readAllBytes(filePath);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        Files.write(getPath(key), value, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        Files.deleteIfExists(getPath(key));
    }

    @Override
    public void close() {
        // No resources to close
    }

    private Path getPath(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key cannot be empty or null");
        }

        String fileName = Base64.getUrlEncoder().encodeToString(key.getBytes());
        return baseDir.resolve(fileName);
    }
}
