package company.vk.edu.distrib.compute.semenmartynov;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Base64;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * File system based implementation of the {@link Dao} interface.
 * Stores data in the directory specified by the "DB_PATH" environment variable,
 * or the "data" folder in the current working directory by default.
 */
public class FileSystemDao implements Dao<byte[]> {
    private static final String ENV_DB_PATH = "DB_PATH";
    private final Path storageDir;

    /**
     * Initializes the file system storage.
     * Determines the storage path and creates the necessary directories if they do not exist.
     *
     * @throws IOException if an I/O error occurs while creating the directory
     */
    public FileSystemDao() throws IOException {
        String envPath = System.getenv(ENV_DB_PATH);
        if (envPath != null && !envPath.isBlank()) {
            this.storageDir = Paths.get(envPath);
        } else {
            // By default, create a "data" folder in CWD
            this.storageDir = Paths.get("data");
        }

        if (!Files.exists(this.storageDir)) {
            Files.createDirectories(this.storageDir);
        }
    }

    /**
     * Resolves the secure file path for a given key.
     * Encodes the key using URL-safe Base64 to prevent directory traversal attacks
     * and handle invalid filename characters across different OS safely.
     *
     * @param key the unique identifier
     * @return the resolved file path
     * @throws IllegalArgumentException if the key is null or empty
     */
    private Path resolvePath(String key) throws IllegalArgumentException {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        String safeFileName = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(key.getBytes(StandardCharsets.UTF_8));
        return storageDir.resolve(safeFileName);
    }

    /**
     * Retrieves the value associated with the given key from the file system.
     *
     * @param key the unique identifier
     * @return the byte array stored in the file
     * @throws NoSuchElementException   if the file representing the key is not found
     * @throws IllegalArgumentException if the key is null or empty
     * @throws IOException              if an I/O error occurs reading from the file
     */
    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        Path filePath = resolvePath(key);
        try {
            return Files.readAllBytes(filePath);
        } catch (NoSuchFileException e) {
            // NoSuchFileException is caught to avoid Time-of-Check to Time-of-Use (TOCTOU) race conditions
            throw new NoSuchElementException("Key not found: " + key, e);
        }
    }

    /**
     * Creates or updates the value for the given key on the file system.
     * Uses an atomic move operation from a temporary file to ensure data consistency 
     * during concurrent writes or sudden crashes.
     *
     * @param key   the unique identifier
     * @param value the data to store
     * @throws IllegalArgumentException if the key is null or empty, or value is null
     * @throws IOException              if an I/O error occurs writing to the file
     */
    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        Path targetPath = resolvePath(key);
        // Use a unique UUID to prevent concurrent upserts from overwriting each other's tmp files
        Path tempPath = storageDir.resolve(targetPath.getFileName().toString() + "_" + UUID.randomUUID() + ".tmp");

        Files.write(tempPath, value);
        try {
            // Atomic move ensures readers always see a complete file, never partially written bytes
            Files.move(tempPath, targetPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            Files.deleteIfExists(tempPath); // Cleanup in case the move fails
            throw e;
        }
    }

    /**
     * Deletes the file associated with the given key if it exists.
     *
     * @param key the unique identifier
     * @throws IllegalArgumentException if the key is null or empty
     * @throws IOException              if an I/O error occurs deleting the file
     */
    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        Path filePath = resolvePath(key);
        Files.deleteIfExists(filePath);
    }

    /**
     * Closes the DAO.
     * For this file system implementation, no persistent descriptors are held, so it acts as a no-op.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        // Nothing to clean up
    }
}
