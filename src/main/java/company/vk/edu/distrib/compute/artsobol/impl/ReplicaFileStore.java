package company.vk.edu.distrib.compute.artsobol.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

final class ReplicaFileStore {
    private static final String ENTRY_GLOB = "*.entry";
    private static final int LOCK_STRIPES = 64;

    private final Path basePath;
    private final ReadWriteLock[] keyLocks = initKeyLocks();
    private final AtomicLong readCount = new AtomicLong();
    private final AtomicLong writeCount = new AtomicLong();
    private final AtomicLong deleteCount = new AtomicLong();

    ReplicaFileStore(Path basePath) {
        this.basePath = basePath;
        createDirectory(basePath);
    }

    VersionedEntry read(String key) throws IOException {
        ReadWriteLock lock = lockFor(key);
        lock.readLock().lock();
        try {
            readCount.incrementAndGet();
            Path entryPath = pathFor(key);
            if (Files.notExists(entryPath)) {
                return null;
            }
            return deserialize(Files.readAllBytes(entryPath));
        } catch (NoSuchFileException e) {
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    boolean writeIfNewer(String key, VersionedEntry entry, boolean count) throws IOException {
        ReadWriteLock lock = lockFor(key);
        lock.writeLock().lock();
        try {
            Path entryPath = pathFor(key);
            VersionedEntry current = readCurrent(entryPath);
            if (current != null && !VersionedEntry.isNewer(entry, current)) {
                return false;
            }
            Path tempPath = Files.createTempFile(basePath, "tmp-", ".tmp");
            try {
                Files.write(tempPath, serialize(entry));
                Files.move(tempPath, entryPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } finally {
                Files.deleteIfExists(tempPath);
            }

            if (count) {
                incrementWriteCounter(entry);
            }
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    ReplicaStats stats(int replicaId, boolean enabled) throws IOException {
        int totalKeys = 0;
        int liveKeys = 0;
        int tombstones = 0;
        long bytes = 0L;

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(basePath, ENTRY_GLOB)) {
            for (Path entryPath : stream) {
                VersionedEntry entry = readCurrent(entryPath);
                if (entry == null) {
                    continue;
                }

                totalKeys++;
                if (entry.tombstone()) {
                    tombstones++;
                } else {
                    liveKeys++;
                    byte[] body = entry.body();
                    bytes += body == null ? 0L : body.length;
                }
            }
        }

        return new ReplicaStats(replicaId, enabled, totalKeys, liveKeys, tombstones, bytes);
    }

    ReplicaAccessStats accessStats(int replicaId) {
        return new ReplicaAccessStats(
                replicaId,
                readCount.get(),
                writeCount.get(),
                deleteCount.get()
        );
    }

    private void incrementWriteCounter(VersionedEntry entry) {
        if (entry.tombstone()) {
            deleteCount.incrementAndGet();
        } else {
            writeCount.incrementAndGet();
        }
    }

    private Path pathFor(String key) {
        return basePath.resolve(hashKey(key) + ".entry");
    }

    private static VersionedEntry readCurrent(Path entryPath) throws IOException {
        if (Files.notExists(entryPath)) {
            return null;
        }
        try {
            return deserialize(Files.readAllBytes(entryPath));
        } catch (NoSuchFileException e) {
            return null;
        }
    }

    private ReadWriteLock lockFor(String key) {
        int index = Math.floorMod(key.hashCode(), keyLocks.length);
        return keyLocks[index];
    }

    private static byte[] serialize(VersionedEntry entry) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (DataOutputStream data = new DataOutputStream(output)) {
            byte[] value = entry.body();
            data.writeLong(entry.version());
            data.writeBoolean(entry.tombstone());
            data.writeInt(value == null ? -1 : value.length);
            if (value != null) {
                data.write(value);
            }
        }
        return output.toByteArray();
    }

    private static VersionedEntry deserialize(byte[] bytes) throws IOException {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
            long version = input.readLong();
            boolean tombstone = input.readBoolean();
            int length = input.readInt();
            byte[] value = null;
            if (length >= 0) {
                value = new byte[length];
                input.readFully(value);
            }
            return new VersionedEntry(version, tombstone, value);
        }
    }

    private static String hashKey(String key) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
        byte[] hash = digest.digest(key.getBytes(StandardCharsets.UTF_8));
        StringBuilder builder = new StringBuilder(hash.length * 2);
        for (byte value : hash) {
            builder.append(String.format("%02x", value));
        }
        return builder.toString();
    }

    private static ReadWriteLock[] initKeyLocks() {
        ReadWriteLock[] locks = new ReadWriteLock[LOCK_STRIPES];
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new ReentrantReadWriteLock();
        }
        return locks;
    }

    private static void createDirectory(Path path) {
        try {
            Files.createDirectories(path);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create replica directory", e);
        }
    }
}
