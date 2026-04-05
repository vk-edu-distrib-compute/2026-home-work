package company.vk.edu.distrib.compute.tadzhnahal;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HexFormat;
import java.util.NoSuchElementException;

public class FileDao implements Dao<byte[]> {
    private final Path rootDir;
    private boolean closed;

    public FileDao(Path rootDir) throws IOException {
        this.rootDir = rootDir;
        Files.createDirectories(rootDir);
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        checkOpened();
        validateKey(key);

        Path path = resolvePath(key);
        if (!Files.exists(path)) {
            throw new NoSuchElementException("No value for key: " + key);
        }

        return Files.readAllBytes(path);
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        checkOpened();
        validateKey(key);

        if (value == null) {
            throw new IllegalArgumentException("Value must not be null");
        }

        Path path = resolvePath(key);
        Files.write(
                path,
                value,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
        );
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        checkOpened();
        validateKey(key);

        Path path = resolvePath(key);
        Files.deleteIfExists(path);
    }

    @Override
    public void close() {
        closed = true;
        // чтобы dao не работал после close
    }

    private void checkOpened() {
        if (closed) {
            throw new IllegalStateException("Dao is closed");
        }
    }

    private void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be null or empty");
        }
    }

    private Path resolvePath(String key) {
        String fileName = HexFormat.of().formatHex(key.getBytes(StandardCharsets.UTF_8)) + ".bin";
        return rootDir.resolve(fileName);
    }
}
