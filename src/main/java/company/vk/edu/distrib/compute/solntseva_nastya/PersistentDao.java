package company.vk.edu.distrib.compute.solntseva_nastya;

import company.vk.edu.distrib.compute.Dao;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;

public class PersistentDao implements Dao<byte[]> {
    private final Path baseDir;

    public PersistentDao(Path baseDir) throws IOException {
        this.baseDir = baseDir;
        if (!Files.exists(baseDir)) {
            Files.createDirectories(baseDir);
        }
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IOException {
        Path filePath = baseDir.resolve(key);
        if (!Files.exists(filePath)) {
            throw new NoSuchElementException("Key not found: " + key);
        }
        return Files.readAllBytes(filePath);
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        Files.write(baseDir.resolve(key), value);
    }

    @Override
    public void delete(String key) throws IOException {
        Files.deleteIfExists(baseDir.resolve(key));
    }

    @Override
    public void close() {
        // No resources to close in this implementation
    }
}
