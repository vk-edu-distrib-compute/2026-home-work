package company.vk.edu.distrib.compute.handlest.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class HandlestBytePayloadFileSystemStorage implements HandlestStorage<byte[]> {
    private final Path basePath;

    private Path resolveKey(String key) {
        return basePath.resolve(key);
    }

    public HandlestBytePayloadFileSystemStorage(String baseDirPath) {
        this.basePath = Paths.get(baseDirPath);
        try {
            if (!Files.exists(basePath)) {
                Files.createDirectories(basePath);
            }

            if (!Files.isDirectory(basePath)) {
                throw new IllegalArgumentException("Path exists but is not a directory: " + baseDirPath);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize storage directory", e);
        }
    }

    @Override
    public byte[] get(String key) {
        Path file = resolveKey(key);
        if (Files.exists(file)) {
            try {
                return Files.readAllBytes(file);
            } catch (IOException e) {
                throw new RuntimeException("Failed to read file for key: " + key, e);
            }
        }
        return null;
    }

    @Override
    public void put(String key, byte[] value) {
        Path file = resolveKey(key);
        try {
            Files.write(file, value);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write file for key: " + key, e);
        }
    }

    @Override
    public void remove(String key) {
        Path file = basePath.resolve(key);
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            throw new RuntimeException("Failed to delete file for key: " + key, e);
        }
    }

    @Override
    public void clear() {
        try (Stream<Path> files = Files.list(basePath)) {
            files.forEach(file -> {
                try {
                    Files.deleteIfExists(file);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to delete file during clear: " + file, e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException("Failed to list directory for clear", e);
        }
    }
}
