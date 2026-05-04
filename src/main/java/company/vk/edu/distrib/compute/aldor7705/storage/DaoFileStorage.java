package company.vk.edu.distrib.compute.aldor7705.storage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

public class DaoFileStorage {

    private final Path path;

    public Path getPath() {
        return path;
    }

    public DaoFileStorage(Path path) {
        this.path = path;
        try {
            if (!Files.exists(path)) {
                Files.createDirectories(path);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Ошибка при создании папки", e);
        }
    }

    public void dropStorage() {
        try {
            if (Files.exists(path)) {
                try (var files = Files.walk(path)) {
                    files.sorted(Comparator.reverseOrder())
                            .forEach(f -> {
                                try {
                                    Files.deleteIfExists(f);
                                } catch (IOException e) {
                                    throw new UncheckedIOException("Ошибка при очистке хранилища", e);
                                }
                            });
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Ошибка при очистке хранилища", e);
        }
    }

    public void save(String key, byte[] value) {
        checkKey(key);
        Path filePath = path.resolve(key);
        try {
            Files.write(filePath, value, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException("Ошибка при сохранении", e);
        }
    }

    public byte[] readFromFile(String key) {
        checkKey(key);
        Path filePath = path.resolve(key);

        if (!Files.exists(filePath)) {
            throw new NoSuchElementException("Элемент с ключом " + key + " не найден");
        }

        try {
            return Files.readAllBytes(filePath);
        } catch (IOException e) {
            throw new UncheckedIOException("Ошибка при чтении файла", e);
        }
    }

    public void deleteFromFile(String key) {
        try {
            Path filePath = path.resolve(key);
            Files.deleteIfExists(filePath);
        } catch (IOException e) {
            throw new UncheckedIOException("Ошибка при удалении записи", e);
        }
    }

    public List<String> getAllKeys() throws IOException {
        return Files.list(path)
                .filter(Files::isRegularFile)
                .map(p -> p.getFileName().toString())
                .toList();
    }

    private void checkKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Ключ должен быть не пустым");
        }
    }
}
