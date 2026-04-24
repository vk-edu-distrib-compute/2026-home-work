package company.vk.edu.distrib.compute.ce_fello;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

final class CeFelloReplicaFileDao {
    private static final String TEMP_SUFFIX = ".tmp";

    private final Path rootDirectory;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicBoolean closed = new AtomicBoolean();

    CeFelloReplicaFileDao(Path rootDirectory) throws IOException {
        this.rootDirectory = Objects.requireNonNull(rootDirectory, "rootDirectory");
        Files.createDirectories(rootDirectory);
    }

    Optional<CeFelloReplicaRecord> read(String key) throws IOException {
        validateKey(key);
        lock.readLock().lock();
        try {
            ensureOpen();
            byte[] bytes = Files.readAllBytes(filePath(key));
            return Optional.of(CeFelloReplicaRecordCodecHelper.decode(bytes));
        } catch (NoSuchFileException e) {
            return Optional.empty();
        } finally {
            lock.readLock().unlock();
        }
    }

    WriteMetadata write(String key, CeFelloReplicaRecord record) throws IOException {
        validateKey(key);
        Objects.requireNonNull(record, "record");

        lock.writeLock().lock();
        try {
            ensureOpen();
            Path targetPath = filePath(key);
            long previousBytes = Files.exists(targetPath) ? Files.size(targetPath) : 0L;
            boolean existed = previousBytes > 0L || Files.exists(targetPath);

            byte[] payload = CeFelloReplicaRecordCodecHelper.encode(record);
            Path tempPath = rootDirectory.resolve(targetPath.getFileName() + TEMP_SUFFIX);
            Files.write(tempPath, payload);
            moveReplacing(tempPath, targetPath);
            return new WriteMetadata(existed, previousBytes, payload.length);
        } finally {
            lock.writeLock().unlock();
        }
    }

    ScanMetadata scan() throws IOException {
        lock.readLock().lock();
        try {
            ensureOpen();
            long keyCount = 0;
            long totalBytes = 0;
            long maxVersion = 0;

            try (Stream<Path> paths = Files.list(rootDirectory)) {
                for (Path path : (Iterable<Path>) paths::iterator) {
                    if (!Files.isRegularFile(path)) {
                        continue;
                    }

                    keyCount++;
                    totalBytes += Files.size(path);
                    CeFelloReplicaRecord record = CeFelloReplicaRecordCodecHelper.decode(Files.readAllBytes(path));
                    maxVersion = Math.max(maxVersion, record.version());
                }
            }

            return new ScanMetadata(keyCount, totalBytes, maxVersion);
        } finally {
            lock.readLock().unlock();
        }
    }

    void close() {
        closed.set(true);
    }

    private Path filePath(String key) {
        return rootDirectory.resolve(encodeKey(key));
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("DAO is already closed");
        }
    }

    private static void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key must be non-empty");
        }
    }

    private static String encodeKey(String key) {
        return Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(key.getBytes(StandardCharsets.UTF_8));
    }

    private static void moveReplacing(Path sourcePath, Path targetPath) throws IOException {
        try {
            Files.move(sourcePath, targetPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    record WriteMetadata(boolean existed, long previousBytes, long currentBytes) {
    }

    record ScanMetadata(long keyCount, long totalBytes, long maxVersion) {
    }
}
