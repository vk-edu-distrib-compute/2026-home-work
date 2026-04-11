package company.vk.edu.distrib.compute.polozhentsev_ivan;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Base64;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

public final class FileSystemDao implements Dao<byte[]> {

    private final Path root;
    private final ReentrantLock lock = new ReentrantLock();

    public FileSystemDao(Path root) throws IOException {
        this.root = Objects.requireNonNull(root);
        Files.createDirectories(this.root);
    }

    private static void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("invalid key");
        }
    }

    private Path fileFor(String key) {
        String name = Base64.getUrlEncoder().withoutPadding().encodeToString(key.getBytes(StandardCharsets.UTF_8));
        return root.resolve(name);
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        validateKey(key);
        Path file = fileFor(key);
        lock.lock();
        try {
            if (!Files.isRegularFile(file)) {
                throw new NoSuchElementException(key);
            }
            return Files.readAllBytes(file);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        validateKey(key);
        byte[] data = Objects.requireNonNullElse(value, new byte[0]);
        Path file = fileFor(key);
        lock.lock();
        try {
            Path tmp = root.resolve(file.getFileName().toString() + ".tmp");
            try {
                Files.write(tmp, data);
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
            } finally {
                Files.deleteIfExists(tmp);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        validateKey(key);
        Path file = fileFor(key);
        lock.lock();
        try {
            Files.deleteIfExists(file);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        // No file handles or channels held open between operations.
    }
}
