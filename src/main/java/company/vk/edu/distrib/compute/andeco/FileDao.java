package company.vk.edu.distrib.compute.andeco;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;

public class FileDao implements Dao<byte[]> {

    private final Path baseDir;

    public FileDao(String path) throws IOException {
        this.baseDir = Path.of("..", "data_" + path);
        Files.createDirectories(baseDir);
    }

    public FileDao(Path baseDir) throws IOException {
        this.baseDir = baseDir;
        Files.createDirectories(baseDir);
    }

    @Override
    public byte[] get(String key) throws IOException {
        Path file = filePath(key);
        if (!Files.exists(file)) {
            throw new NoSuchElementException();
        }
        return Files.readAllBytes(file);
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        Path file = filePath(key);
        Files.write(file, value);
    }

    @Override
    public void delete(String key) throws IOException {
        Path file = filePath(key);
        Files.deleteIfExists(file);
    }

    @Override
    public void close() {
        // нет подключённых ресурсов
    }

    private Path filePath(String key) {
        return baseDir.resolve(key);
    }

    public int size() throws IOException {
        try (var stream = Files.list(baseDir)) {
            return (int) stream.filter(Files::isRegularFile).count();
        }
    }
}
