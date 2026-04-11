package company.vk.edu.distrib.compute;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Base64;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Реализация DAO для хранения данных в файловой системе.
 * Каждый ключ сохраняется в отдельный файл в указанной директории, запись происходит атомарно через временный файл.
 * Потокобезопасна: операции синхронизируются по ключу, временные файлы автоматически очищаются при старте.
 */
public class FileSystemDao implements Dao<byte[]> {
    private static final Logger LOG = LoggerFactory.getLogger(FileSystemDao.class);
    private static final String TEMP_SUFFIX = ".tmp";
    private final Path storageDir;
    //Блокировки для предотвращения гонок
    private final Map<String, ReentrantLock> keyLocks = new ConcurrentHashMap<>();

    public FileSystemDao(final String storageDirPath) throws IOException {
        this.storageDir = Paths.get(storageDirPath);
        if (!Files.exists(storageDir)) {
            Files.createDirectories(storageDir); //Если директория не существует, создаёт её вместе со родителем
            LOG.info("Created storage directory: {}", storageDir);
        }
        if (!Files.isDirectory(storageDir)) {
            throw new IllegalArgumentException("Path exists but is not a directory: " + storageDirPath);
        }
        //Удалить все временные файлы .tmp при старте (оставшиеся от предыдущих сбоев)
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(storageDir,
                path -> path.toString().endsWith(TEMP_SUFFIX))) {
            for (Path tmpFile : stream) {
                Files.deleteIfExists(tmpFile);
                LOG.debug("Cleaned up stale temporary file: {}", tmpFile);
            }
        }
    }

    private Path getFileForKey(final String key) {
        //Кодирует ключ в URL-совместимый формат (заменяет спец.символы, например пробелы на %20)
        String encodedKey = Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(key.getBytes(StandardCharsets.UTF_8));
        return storageDir.resolve(encodedKey); //Склеить имя папки и имя файла
    }

    //Метод для получения блокировки по ключу. computeIfAbsent гарантирует потокобезопасное создание
    private ReentrantLock getLockForKey(String key) {
        return keyLocks.computeIfAbsent(key, k -> new ReentrantLock());
    }

    //Удаляем блокировку, если она не захвачена и нет ожидающих её потоков (если пара ключ-значение совпадает)
    private void cleanupLock(String key, ReentrantLock lock) {
        if (!lock.isLocked() && !lock.hasQueuedThreads()) {
            keyLocks.remove(key, lock);
        }
    }

    @Override
    public byte[] get(final String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        validateKey(key);
        ReentrantLock lock = getLockForKey(key);
        lock.lock();
        try {
            final Path file = getFileForKey(key);
            if (!Files.exists(file)) {
                throw new NoSuchElementException("No data for key: " + key);
            }
            return Files.readAllBytes(file);
        } finally {
            lock.unlock();
            cleanupLock(key, lock);
        }
    }

    @Override
    public void upsert(final String key, final byte[] value) throws IllegalArgumentException, IOException {
        validateKey(key);
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        ReentrantLock lock = getLockForKey(key);
        lock.lock();
        try {
            final Path target = getFileForKey(key);
            final Path temp = Files.createTempFile(storageDir, key, TEMP_SUFFIX);
            try {
                Files.write(temp, value);
                //Запись в tmp file и ACID замена по готоности
                Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING); //
            } catch (IOException e) {
                Files.deleteIfExists(temp);
                throw e;
            }
            LOG.debug("Upserted key: {}", key);
        } finally {
            lock.unlock();
            cleanupLock(key, lock);
        }
    }

    @Override
    public void delete(final String key) throws IllegalArgumentException, IOException {
        validateKey(key);
        ReentrantLock lock = getLockForKey(key);
        lock.lock();
        try {
            final Path file = getFileForKey(key);
            Files.deleteIfExists(file);
            LOG.debug("Deleted key: {}", key);
        } finally {
            lock.unlock();
            cleanupLock(key, lock);
        }
    }

    @Override
    public void close() throws IOException {
        keyLocks.clear();
        LOG.debug("FileSystemDao closed");
    }

    private static void validateKey(final String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must be non-empty");
        }
    }
}
