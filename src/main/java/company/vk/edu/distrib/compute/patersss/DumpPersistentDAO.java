package company.vk.edu.distrib.compute.patersss;

import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

public class DumpPersistentDAO implements Dao<byte[]> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DumpPersistentDAO.class);
    private final ReentrantLock lock = new ReentrantLock();

    private static final String STORAGE_FILE_NAME = "data.db";
    private static final int TOMBSTONE = -1;
    private final RandomAccessFile storage;
    private final ConcurrentMap<String, RecordMeta> index = new ConcurrentHashMap<>();

    public DumpPersistentDAO(Path basePath) throws IOException {
        Files.createDirectories(basePath);
        Path storagePath = basePath.resolve(STORAGE_FILE_NAME);
        this.storage = new RandomAccessFile(storagePath.toFile(), "rw");
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Opening DAO at {}", storagePath.toAbsolutePath());
        }
        loadIndex();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("DAO initialized, keys in index: {}", index.size());
        }
    }

    @Override
    public byte[] get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        keyCheck(key);

        RecordMeta meta = index.get(key);
        if (meta == null) {
            LOGGER.info("GET missed for key={}", key);
            throw new NoSuchElementException("There is no element with key " + key);
        }

        lock.lock();
        try {
            storage.seek(meta.valueOffset());
            byte[] value = new byte[meta.valueSize()];
            storage.readFully(value);

            LOGGER.info("GET success for key={}, bytes={}", key, value.length);

            return value;
        } catch (IOException e) {
            LOGGER.error("GET failed for key={}", key, e);
            throw e;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void upsert(String key, byte[] value) throws IllegalArgumentException, IOException {
        keyCheck(key);
        if (value == null) {
            LOGGER.info("Value is null for key {}", key);
            throw new IllegalArgumentException("Value is null");
        }

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

        lock.lock();
        try {
            storage.seek(storage.length());
            storage.writeInt(keyBytes.length);
            storage.writeInt(value.length);
            storage.write(keyBytes);

            long valueOffset = storage.getFilePointer();
            storage.write(value);

            index.put(key, new RecordMeta(valueOffset, value.length));

            LOGGER.info("UPSERT success for key={}, bytes={}", key, value.length);
        } catch (IOException e) {
            LOGGER.error("UPSERT failed for key={}", key, e);
            throw e;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        keyCheck(key);

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

        lock.lock();
        try {
            storage.seek(storage.length());
            storage.writeInt(keyBytes.length);
            storage.writeInt(TOMBSTONE);
            storage.write(keyBytes);

            index.remove(key);

            LOGGER.info("DELETE success for key={}", key);
        } catch (IOException e) {
            LOGGER.error("DELETE failed for key={}", key, e);
            throw e;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            LOGGER.info("Closing DAO");
            storage.getFD().sync();
            storage.close();
            LOGGER.info("DAO closed");
        } catch (IOException e) {
            LOGGER.error("Failed to close DAO", e);
            throw e;
        } finally {
            lock.unlock();
        }
    }

    private void loadIndex() throws IOException {
        lock.lock();
        try {
            storage.seek(0);
            while (storage.getFilePointer() < storage.length()) {
                loadNextRecord();
            }
        } finally {
            lock.unlock();
        }
    }

    private void loadNextRecord() throws IOException {
        long recordStart = storage.getFilePointer();

        ensureAvailable(Integer.BYTES * 2, recordStart, "header");

        final int keySize = storage.readInt();
        final int valueSize = storage.readInt();

        validateKeySize(keySize, recordStart);

        ensureAvailable(keySize, recordStart, "key");
        byte[] keyBytes = new byte[keySize];
        storage.readFully(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);

        if (valueSize == TOMBSTONE) {
            index.remove(key);
            LOGGER.info("Loaded tombstone for key={}", key);
            return;
        }

        validateValueSize(valueSize, recordStart);
        ensureAvailable(valueSize, recordStart, "value");

        long valueOffset = storage.getFilePointer();
        storage.seek(valueOffset + valueSize);
        index.put(key, new RecordMeta(valueOffset, valueSize));
        LOGGER.info("Loaded record for key={}, bytes={}", key, valueSize);
    }

    private void ensureAvailable(long bytesNeeded, long recordStart, String part) throws IOException {
        long remaining = storage.length() - storage.getFilePointer();
        if (remaining < bytesNeeded) {
            throw new IOException("Corrupted storage: not enough bytes for " + part + " at offset " + recordStart);
        }
    }

    private static void validateKeySize(int keySize, long recordStart) throws IOException {
        if (keySize <= 0) {
            throw new IOException("Corrupted storage: invalid key size at offset " + recordStart);
        }
    }

    private static void validateValueSize(int valueSize, long recordStart) throws IOException {
        if (valueSize < 0) {
            throw new IOException("Corrupted storage: invalid value size at offset " + recordStart);
        }
    }

    private void keyCheck(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Bad key. Null or empty");
        }
    }

    private record RecordMeta(long valueOffset, int valueSize) {
    }
}
