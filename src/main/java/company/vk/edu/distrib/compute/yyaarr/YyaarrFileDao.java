package company.vk.edu.distrib.compute.yyaarr;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;

public class YyaarrFileDao implements Dao<byte[]> {
    private static final Path STORAGE_DIR = Paths.get("src/main/java/company/vk/edu/distrib/compute/yyaarr/data_"
            + ThreadLocalRandom.current().nextInt(1_000_000, 10_000_000));
    private static final String DATSUFFIX = ".dat";

    private void createDirectory() throws IOException {
        if (!Files.exists(STORAGE_DIR)) {
            Files.createDirectories(STORAGE_DIR);
        }
    }

    YyaarrFileDao() throws IOException {
            createDirectory();
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        checkKey(key);
        if (Files.exists(STORAGE_DIR.resolve(key + DATSUFFIX))) {
            return Files.readAllBytes(STORAGE_DIR.resolve(key + DATSUFFIX));
        }
        throw new NoSuchElementException("key not found");
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        checkKey(key);
        Files.write(STORAGE_DIR.resolve(key + DATSUFFIX), value);
        if (!Files.exists(STORAGE_DIR.resolve(key + DATSUFFIX))) {
            throw new IOException("File with data has not been created");
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        checkKey(key);
        if (Files.exists(STORAGE_DIR.resolve(key + DATSUFFIX))) {
            Files.delete(STORAGE_DIR.resolve(key + DATSUFFIX));
        }
        if (Files.exists(STORAGE_DIR.resolve(key + DATSUFFIX))) {
            throw new IOException("File has not been deleted");
        }
    }

    @Override
    public void close() throws IOException {
        //no need
    }

    private void checkKey(String key) throws IllegalArgumentException {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key is null or blank");
        }
    }
}
