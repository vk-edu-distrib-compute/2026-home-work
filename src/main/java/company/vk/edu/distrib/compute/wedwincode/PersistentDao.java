package company.vk.edu.distrib.compute.wedwincode;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Base64;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

public class PersistentDao implements Dao<DaoRecord> {
    private final Path storageDir;
    private final ConcurrentMap<String, ReentrantLock> keyLocks;

    public PersistentDao(Path storageDir) throws IOException {
        this.storageDir = storageDir;
        this.keyLocks = new ConcurrentHashMap<>();
        Files.createDirectories(storageDir);
    }

    @Override
    public DaoRecord get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        checkKey(key);
        ReentrantLock lock = keyLocks.computeIfAbsent(key, k -> new ReentrantLock());
        lock.lock();
        try {
            Path file = resolvePath(key);
            if (!Files.exists(file)) {
                throw new NoSuchElementException("file not found: " + key);
            }
            return deserialize(Files.readAllBytes(file));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void upsert(String key, DaoRecord daoRecord) throws IllegalArgumentException, IOException {
        checkKey(key);
        if (daoRecord == null) {
            throw new IllegalArgumentException("value must not be null");
        }

        ReentrantLock lock = keyLocks.computeIfAbsent(key, k -> new ReentrantLock());
        lock.lock();
        try {
            Path file = resolvePath(key);
            Path tempFile = Files.createTempFile(storageDir, "tmp-", ".bin");

            try {
                Files.write(tempFile, serialize(daoRecord));

                try {
                    Files.move(tempFile, file,
                            StandardCopyOption.REPLACE_EXISTING,
                            StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(tempFile, file,
                            StandardCopyOption.REPLACE_EXISTING);
                }
            } finally {
                Files.deleteIfExists(tempFile);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        checkKey(key);
        ReentrantLock lock = keyLocks.computeIfAbsent(key, k -> new ReentrantLock());
        lock.lock();
        try {
            Path file = resolvePath(key);
            Files.write(file, serialize(DaoRecord.buildDeleted()));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        // not needed in PersistentDao
    }

    private Path resolvePath(String key) {
        String fileName = Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(key.getBytes(StandardCharsets.UTF_8));
        return storageDir.resolve(fileName);
    }

    private void checkKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key must not be null or empty");
        }
    }

    private byte[] serialize(DaoRecord daoRecord) {
        byte[] data = daoRecord.data() == null ? new byte[0] : daoRecord.data();

        int size = Long.BYTES
                + 1
                + Integer.BYTES
                + data.length;

        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putLong(daoRecord.timestamp());
        buffer.put((byte) (daoRecord.deleted() ? 1 : 0));
        buffer.putInt(data.length);
        buffer.put(data);

        return buffer.array();
    }

    private DaoRecord deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        long timestamp = buffer.getLong();
        boolean deleted = buffer.get() == 1;
        int length = buffer.getInt();

        byte[] data = new byte[length];
        buffer.get(data);

        return new DaoRecord(data, timestamp, deleted);
    }
}
