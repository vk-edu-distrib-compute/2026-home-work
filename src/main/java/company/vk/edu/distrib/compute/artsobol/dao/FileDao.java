package company.vk.edu.distrib.compute.artsobol.dao;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.NoSuchElementException;

public class FileDao implements Dao<byte[]> {

    private final Path basePath;

    public FileDao(Path path) {
        basePath = path;
        createDirectory(basePath);
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        validateKey(key);
        Path path = getPathToFile(key);
        try {
            return read(path);
        } catch (NoSuchFileException e) {
            throw new NoSuchElementException("Key not found: " + key, e);
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        validateKey(key);
        Path path = getPathToFile(key);
        write(path, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        validateKey(key);
        Path path = getPathToFile(key);
        delete(path);
    }

    @Override
    public void close() {
        // Nothing to close
    }

    private static void delete(Path path) throws IOException {
        Files.deleteIfExists(path);
    }

    private static byte[] read(Path path) throws IOException {
        return Files.readAllBytes(path);
    }

    private void write(Path path, byte[] value) throws IOException {
        Path temp = Files.createTempFile(basePath, "tmp-", ".tmp");
        try {
            Files.write(temp, value);
            Files.move(temp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    private Path getPathToFile(String key) {
        String hashKey = createHashKey(key);
        return basePath.resolve(hashKey);
    }

    private static String createHashKey(String key) {
        byte[] hash = createHash(key);
        return toHex(hash);
    }

    private static String toHex(byte[] hash) {
        StringBuilder builder = new StringBuilder(hash.length * 2);
        for (byte b : hash) {
            builder.append(String.format("%02x", b));
        }

        return builder.toString();
    }

    private static void validateKey(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key must not be null");
        }
    }

    private static byte[] createHash(String key) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException("SHA-256 is not available", e);
        }
        return digest.digest(key.getBytes(StandardCharsets.UTF_8));
    }

    private static void createDirectory(Path path) {
        try {
            Files.createDirectories(path);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create data directory", e);
        }
    }
}
