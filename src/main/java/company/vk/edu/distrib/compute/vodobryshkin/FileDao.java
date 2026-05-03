package company.vk.edu.distrib.compute.vodobryshkin;

import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Dao на основе файловой системы.
 *
 * <p>
 *     Сохраняет всю информацию в директорию storage в рабочей директории.
 *     Значение под каждым ключом хранится в файле вида storage/&ltkey>
 * </p>
 */
public class FileDao implements Dao<byte[]> {
    private static final Logger log = LoggerFactory.getLogger("server");

    private static final Path DEFAULT_ROOT_DIRECTORY = Path.of(System.getProperty("user.home"), "highload-course");

    private final Path rootDirectory;
    private final Map<String, Lock> locks = new ConcurrentHashMap<>();

    public FileDao() throws IOException {
        this(DEFAULT_ROOT_DIRECTORY);
    }

    public FileDao(Path rootDirectory) throws IOException {
        if (!Files.exists(rootDirectory)) {
            Files.createDirectories(rootDirectory.normalize());
        }

        this.rootDirectory = rootDirectory.normalize();
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        Lock lock = lockKey(key);
        lock.lock();

        try {
            return Files.readAllBytes(filePath(key));
        } catch (NoSuchFileException e) {
            log.error("Key \"{}\" doesn't exist in a file system.", key, e);
            throw e;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        Lock lock = lockKey(key);
        lock.lock();

        try {
            Files.write(filePath(key), value);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        Lock lock = lockKey(key);
        lock.lock();

        try {
            try {
                Files.delete(filePath(key));
            } catch (NoSuchFileException e) {
                log.warn("Key \"{}\" doesn't exist in file system.", key);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        //
    }

    private Path filePath(String pathWay) {
        Path result = rootDirectory.resolve(Path.of(pathWay)).normalize();

        if (!result.startsWith(rootDirectory)) {
            throw new IllegalArgumentException("Invalid path name: " + pathWay);
        }

        return result;
    }

    private Lock lockKey(String key) {
        return locks.computeIfAbsent(key, ignored -> new ReentrantLock());
    }
}
