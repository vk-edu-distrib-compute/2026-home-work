package company.vk.edu.distrib.compute.maryarta;

import company.vk.edu.distrib.compute.Dao;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.NoSuchElementException;

public class FileDao implements Dao<byte []> {
    private static final Path STORAGE = Path.of(".storage");

    FileDao() throws IOException {
        Files.createDirectories(STORAGE);
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        if (key.isBlank()) {
            throw new IllegalArgumentException("Key is blank");
        }
        Path file = STORAGE.resolve(keyToHash(key));
        if (Files.exists(file)) {
            return Files.readAllBytes(file);
        }
        throw new NoSuchElementException("No element for key " + key);
    }

    @Override
    public void upsert(String key, byte[] value) throws IOException {
        if (key.isBlank()) {
            throw new IllegalArgumentException("Key is blank");
        }
        String hash = keyToHash(key);
        Path file = STORAGE.resolve(hash);
        Path temp = STORAGE.resolve(hash + ".temp");

        Files.write(temp, value);
        Files.move(
                temp, file,
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE
        );
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        if (key.isBlank()) {
            throw new IllegalArgumentException("Key is blank");
        }
        Files.deleteIfExists(STORAGE.resolve(keyToHash(key)));
    }

    @Override
    public void close() throws IOException {
        //no-op
    }

    private static String keyToHash(String key) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(key.getBytes());
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm is not available", e);
        }
    }

}
