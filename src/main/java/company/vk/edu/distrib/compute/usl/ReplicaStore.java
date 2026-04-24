package company.vk.edu.distrib.compute.usl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Base64;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

final class ReplicaStore {
    private static final String ENTRY_GLOB = "*.entry";
    private static final int LOCK_STRIPES = 64;

    private final Path rootDirectory;
    private final ReadWriteLock[] locks = initLocks();
    private final AtomicLong readRequests = new AtomicLong();
    private final AtomicLong writeRequests = new AtomicLong();
    private final AtomicLong deleteRequests = new AtomicLong();

    ReplicaStore(Path rootDirectory) throws IOException {
        this.rootDirectory = rootDirectory;
        Files.createDirectories(rootDirectory);
    }

    VersionedValue read(String key) throws IOException {
        ReadWriteLock lock = lockFor(key);
        lock.readLock().lock();
        try {
            readRequests.incrementAndGet();
            return readCurrent(filePath(key));
        } finally {
            lock.readLock().unlock();
        }
    }

    boolean writeIfNewer(String key, VersionedValue value, boolean countAccess) throws IOException {
        ReadWriteLock lock = lockFor(key);
        lock.writeLock().lock();
        try {
            Path entryPath = filePath(key);
            VersionedValue currentValue = readCurrent(entryPath);
            if (currentValue != null && !VersionedValue.isNewer(value, currentValue)) {
                return false;
            }

            Path temporaryPath = temporaryPath(entryPath);
            try {
                Files.write(temporaryPath, serialize(value));
                Files.move(
                    temporaryPath,
                    entryPath,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING
                );
            } finally {
                Files.deleteIfExists(temporaryPath);
            }

            if (countAccess) {
                if (value.tombstone()) {
                    deleteRequests.incrementAndGet();
                } else {
                    writeRequests.incrementAndGet();
                }
            }
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    ReplicaStatsSnapshot stats(int replicaId, boolean enabled) throws IOException {
        int liveKeys = 0;
        int tombstones = 0;
        long storedBytes = 0;

        try (DirectoryStream<Path> entries = Files.newDirectoryStream(rootDirectory, ENTRY_GLOB)) {
            for (Path entryPath : entries) {
                VersionedValue value = readCurrent(entryPath);
                if (value == null) {
                    continue;
                }
                if (value.tombstone()) {
                    tombstones++;
                } else {
                    liveKeys++;
                    storedBytes += value.bodyLength();
                }
            }
        }

        return new ReplicaStatsSnapshot(replicaId, enabled, liveKeys, tombstones, storedBytes);
    }

    ReplicaAccessStatsSnapshot accessStats(int replicaId, boolean enabled) {
        return new ReplicaAccessStatsSnapshot(
            replicaId,
            enabled,
            readRequests.get(),
            writeRequests.get(),
            deleteRequests.get()
        );
    }

    private Path filePath(String key) {
        return rootDirectory.resolve(encodedKey(key) + ".entry");
    }

    private static Path temporaryPath(Path entryPath) {
        return entryPath.resolveSibling(
            entryPath.getFileName() + ".tmp-" + Long.toUnsignedString(ThreadLocalRandom.current().nextLong())
        );
    }

    private ReadWriteLock lockFor(String key) {
        return locks[Math.floorMod(key.hashCode(), locks.length)];
    }

    private static VersionedValue readCurrent(Path entryPath) throws IOException {
        try {
            if (Files.notExists(entryPath)) {
                return null;
            }
            return deserialize(Files.readAllBytes(entryPath));
        } catch (NoSuchFileException e) {
            return null;
        }
    }

    private static byte[] serialize(VersionedValue value) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
            byte[] body = value.body();
            outputStream.writeLong(value.version());
            outputStream.writeBoolean(value.tombstone());
            outputStream.writeInt(body == null ? -1 : body.length);
            if (body != null) {
                outputStream.write(body);
            }
        }
        return byteArrayOutputStream.toByteArray();
    }

    private static VersionedValue deserialize(byte[] bytes) throws IOException {
        try (DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(bytes))) {
            long version = inputStream.readLong();
            boolean tombstone = inputStream.readBoolean();
            int bodyLength = inputStream.readInt();
            byte[] body = null;
            if (bodyLength >= 0) {
                body = new byte[bodyLength];
                inputStream.readFully(body);
            }
            return new VersionedValue(version, tombstone, body);
        }
    }

    private static String encodedKey(String key) {
        return Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(key.getBytes(StandardCharsets.UTF_8));
    }

    private static ReadWriteLock[] initLocks() {
        ReadWriteLock[] createdLocks = new ReadWriteLock[LOCK_STRIPES];
        for (int index = 0; index < createdLocks.length; index++) {
            createdLocks[index] = new ReentrantReadWriteLock();
        }
        return createdLocks;
    }
}
