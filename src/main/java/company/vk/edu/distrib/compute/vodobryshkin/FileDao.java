package company.vk.edu.distrib.compute.vodobryshkin;

import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.NoSuchElementException;

/**
 * Dao на основе файловой системы.
 *
 * <p>
 *     Сохраняет всю информацию в директорию storage в рабочей директории.
 *     Значение под каждым ключом хранится в файле вида storage/&ltkey>
 * </p>
 *
 * <p>
 *     Для кэширования запросов на получение значения по ключу дополнительно используется {@code CacheDao}.
 * </p>
 */
public class FileDao implements Dao<byte[]> {
    private static final Logger log = LoggerFactory.getLogger("server");

    private static final Path DEFAULT_ROOT_DIRECTORY = Path.of("storage");
    private static final int DEFAULT_LIMIT = 100;

    private final Path rootDirectory;
    private final Dao<byte[]> cache;

    public FileDao() throws IOException {
        this(DEFAULT_ROOT_DIRECTORY, DEFAULT_LIMIT);
    }

    public FileDao(Path rootDirectory, int cacheLimit) throws IOException {
        if (!Files.exists(rootDirectory)) {
            Files.createDirectories(rootDirectory.normalize());
        }

        this.rootDirectory = rootDirectory.normalize();
        this.cache = new CacheDao(cacheLimit);
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        byte[] value;

        try {
            try {
                value = cache.get(key);
            } catch (NoSuchElementException elementException) {
                value = Files.readAllBytes(filePath(key));
                cache.upsert(key, value);
            }

            return value;
        } catch (NoSuchFileException fileException) {
            log.error("Key \"{}\" doesn't exist in a file system.", key, fileException);
            throw fileException;
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        Files.write(filePath(key), value);
        cache.upsert(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        try {
            Files.delete(filePath(key));
        } catch (NoSuchFileException e) {
            log.warn("Key \"{}\" doesn't exist in file system.", key);
        }

        try {
            cache.delete(key);
        } catch (NoSuchElementException e) {
            log.debug("Key \"{}\" doesn't exist in cache.", key);
        }
    }

    @Override
    public void close() throws IOException {
        cache.close();
    }

    private Path filePath(String pathWay) {
        Path result = rootDirectory.resolve(Path.of(pathWay)).normalize();

        if (!result.startsWith(rootDirectory)) {
            throw new IllegalArgumentException("Invalid path name: " + pathWay);
        }

        return result;
    }
}
