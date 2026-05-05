package company.vk.edu.distrib.compute.nst1610.dao;

import company.vk.edu.distrib.compute.Dao;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.NoSuchElementException;

public class FileDao implements Dao<byte[]> {
    private final Path storageDirectory;

    public FileDao(Path storageDirectory) throws IOException {
        this.storageDirectory = storageDirectory;
        Files.createDirectories(storageDirectory);
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        Path file = resolvePath(key);
        if (!Files.exists(file)) {
            throw new NoSuchElementException("No value for key: " + key);
        }
        return Files.readAllBytes(resolvePath(key));
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        Files.write(
            resolvePath(key),
            value,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE
        );
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        Files.deleteIfExists(resolvePath(key));
    }

    @Override
    public void close() throws IOException {
        // нечего закрывать
    }

    private Path resolvePath(String key) {
        validateKey(key);
        return storageDirectory.resolve(key);
    }

    private static void validateKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key must not be blank or null");
        }
    }
}
