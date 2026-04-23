package company.vk.edu.distrib.compute.ronshinvsevolod;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.Base64;
import java.util.NoSuchElementException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class FileDao implements Dao<byte[]> {
    private final Path rootDir;
    
    public FileDao(String rootDirPath) throws IOException {
        this.rootDir = Paths.get(rootDirPath);
        Files.createDirectories(rootDir);
    }

    private Path getPath(String key) {
        String safeName = Base64.getUrlEncoder().encodeToString(key.getBytes(StandardCharsets.UTF_8));
        return rootDir.resolve(safeName);
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }

        Path path = getPath(key);
        if (!Files.exists(path)) {
            throw new NoSuchElementException("The file does not exist");
        }
        
        return Files.readAllBytes(path);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        Path path = getPath(key);
        Files.write(path, value, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        Path path = getPath(key);
        Files.deleteIfExists(path);
    }

    @Override
    public void close() throws IOException {
        // just nothing
    }
}
