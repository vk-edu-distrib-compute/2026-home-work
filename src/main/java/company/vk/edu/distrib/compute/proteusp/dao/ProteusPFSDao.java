package company.vk.edu.distrib.compute.proteusp.dao;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.NoSuchElementException;

public class ProteusPFSDao implements Dao<byte[]> {

    private final Path storagePath;

    public ProteusPFSDao(Class<?> anchorClass) {

        String location = anchorClass.getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .getPath();

        this.storagePath = Paths.get(location, "data");
        try {
            Files.createDirectories(storagePath);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create storage directory at: " + storagePath, e);
        }
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        Path filePath = storagePath.resolve(key);
        if (!Files.exists(filePath)) {
            throw new NoSuchElementException("Key not found: " + key);
        }
        return Files.readAllBytes(filePath);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        Path filePath = storagePath.resolve(key);
        Files.write(filePath, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        Path filePath = storagePath.resolve(key);
        Files.deleteIfExists(filePath);
    }

    @Override
    public void close() throws IOException {
        // Nothing to close :)
    }
}
