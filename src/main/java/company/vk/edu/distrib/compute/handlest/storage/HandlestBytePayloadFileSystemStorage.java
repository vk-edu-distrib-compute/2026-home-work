package company.vk.edu.distrib.compute.handlest.storage;

import company.vk.edu.distrib.compute.handlest.exceptions.FileException;

import java.io.IOException;
import java.nio.file.*;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

public class HandlestBytePayloadFileSystemStorage implements HandlestStorage<byte[]> {
    private final Path basePath;

    private Path resolveKey(String key) {
        if (key.contains("..")) {
            throw new SecurityException("Can not traverse outside storage folder");
        }

        String sanitizedKey = key;
        if (sanitizedKey.startsWith("/")) {
            sanitizedKey = sanitizedKey.replaceAll("^/+|/+$", "");
        }

        return basePath.resolve(sanitizedKey);
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
            throw new FileException("Failed to initialize storage directory", e);
        }
    }

    @Override
    public byte[] get(String key) {
        Path file = resolveKey(key);
        if (!Files.exists(file)) {
            throw new NoSuchElementException("Key not found: " + key);
        }
        try {
            return Files.readAllBytes(file);
        } catch (IOException e) {
            throw new FileException("Failed to read file for key: " + key, e);
        }
    }

    @Override
    public void put(String key, byte[] value) {
        Path targetFile = resolveKey(key);
        Path tempFile = null;
        try {
            Path parent = targetFile.getParent();
            if (parent != null && !Files.exists(parent)) {
                Files.createDirectories(parent);
            }

            tempFile = Files.createTempFile(
                    parent != null ? parent : basePath,
                    targetFile.getFileName().toString() + ".",
                    ".tmp"
            );

            Files.write(tempFile, value);
            try {
                Files.move(tempFile, targetFile,
                        StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tempFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            if (tempFile != null) {
                try {
                    Files.deleteIfExists(tempFile);
                } catch (IOException ignored) {
                    throw new FileException("Temp file was not deleted", e);
                }
            }
            throw new FileException("Failed to write file for key: " + key, e);
        }
    }

    @Override
    public void remove(String key) {
        Path file = basePath.resolve(key);
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            throw new FileException("Failed to delete file for key: " + key, e);
        }
    }

    @Override
    public void clear() {
        try (Stream<Path> files = Files.list(basePath)) {
            files.forEach(file -> {
                try {
                    Files.deleteIfExists(file);
                } catch (IOException e) {
                    throw new FileException("Failed to delete file during clear: " + file, e);
                }
            });
        } catch (IOException e) {
            throw new FileException("Failed to list directory for clear", e);
        }
    }
}
