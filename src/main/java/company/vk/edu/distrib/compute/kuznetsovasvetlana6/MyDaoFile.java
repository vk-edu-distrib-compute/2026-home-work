package company.vk.edu.distrib.compute.kuznetsovasvetlana6;

import company.vk.edu.distrib.compute.Dao;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.NoSuchElementException;

public class MyDaoFile implements Dao<byte[]> {
    private final Path basePath;

    public MyDaoFile(Path basePath) throws IOException {
        this.basePath = basePath;
        if (!Files.exists(basePath)) {
            Files.createDirectories(basePath);
        }
    }

    @Override
    public byte[] get(String key) throws IOException {
        Path filePath = basePath.resolve(key);
        if (!Files.exists(filePath)) {
            throw new NoSuchElementException("Key not found: " + key);
        }
        return Files.readAllBytes(filePath);
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be empty");
        }
        Path filePath = basePath.resolve(key);
        Files.write(filePath, value, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    @Override
    public void delete(String key) throws IOException {
        Path filePath = basePath.resolve(key);
        Files.deleteIfExists(filePath);
    }

    @Override
    public void close() {
        // Для файлового ничего не делаем
    }
}
