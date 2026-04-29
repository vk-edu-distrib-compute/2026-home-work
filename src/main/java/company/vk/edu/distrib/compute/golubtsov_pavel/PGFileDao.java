package company.vk.edu.distrib.compute.golubtsov_pavel;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.NoSuchElementException;

public class PGFileDao implements Dao<byte[]> {
    private final Path storageDir;

    public PGFileDao(Path storageDir) throws IOException {
        this.storageDir = storageDir;
        Files.createDirectories(storageDir);
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        if ((key == null) || (key.isBlank())) {
            throw new IllegalArgumentException("key is null or blank");
        }
        Path file = storageDir.resolve(key);
        if (!Files.exists(file)) {
            throw new NoSuchElementException("No value for key: " + key);
        }
        return Files.readAllBytes(file);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        if ((key == null) || (key.isBlank())) {
            throw new IllegalArgumentException("key is null or blank");
        }
        if (value == null) {
            throw new IllegalArgumentException("value is null");
        }
        Path file = storageDir.resolve(key);
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        Files.write(tmp, value);
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        if ((key == null) || (key.isBlank())) {
            throw new IllegalArgumentException("key is null or blank");
        }
        Path file = storageDir.resolve(key);
        Files.deleteIfExists(file);
    }

    @Override
    public void close() throws IOException {
        //nothing
    }
}
