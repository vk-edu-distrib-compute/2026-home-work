package company.vk.edu.distrib.compute.bushuev_a_s;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.NoSuchElementException;

public class MyFileDao implements Dao<byte[]> {
    private final Path baseDir;

    public MyFileDao(Path baseDir) throws IOException {
        this.baseDir = baseDir;
        if (!Files.exists(baseDir)) {
            Files.createDirectories(baseDir);
        }
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        Path filePath = baseDir.resolve(key);
        if (!Files.exists(filePath)) {
            throw new NoSuchElementException("Entity not found: " + key);
        }
        try {
            return Files.readAllBytes(filePath);
        } catch (IOException e) {
            throw new DaoException("Failed to read file", e);
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        Path filePath = baseDir.resolve(key);
        try {
            Files.write(filePath, value,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new DaoException("Failed to write file", e);
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        Path filePath = baseDir.resolve(key);
        try {
            Files.deleteIfExists(filePath);
        } catch (IOException e) {
            throw new DaoException("Failed to delete file", e);
        }
    }

    @Override
    public void close() throws IOException {
        //yet empty
    }

    public class DaoException extends RuntimeException {
        public DaoException(String message, Throwable cause) {
            super(message, cause);
        }
    }

}
