package company.vk.edu.distrib.compute.ce_fello;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

final class CeFelloReplicaNode {
    private final int replicaId;
    private final CeFelloReplicaFileDao dao;
    private final AtomicBoolean enabled = new AtomicBoolean(true);
    private final AtomicLong storedKeyCount = new AtomicLong();
    private final AtomicLong storedBytes = new AtomicLong();
    private final AtomicLong totalReadCount = new AtomicLong();
    private final AtomicLong totalWriteCount = new AtomicLong();
    private final AtomicLong totalDeleteCount = new AtomicLong();

    CeFelloReplicaNode(int id, Path rootDirectory) throws IOException {
        this.replicaId = id;
        this.dao = new CeFelloReplicaFileDao(Objects.requireNonNull(rootDirectory, "rootDirectory"));

        CeFelloReplicaFileDao.ScanMetadata metadata = dao.scan();
        this.storedKeyCount.set(metadata.keyCount());
        this.storedBytes.set(metadata.totalBytes());
    }

    int id() {
        return replicaId;
    }

    long initializeMaxVersion() throws IOException {
        return dao.scan().maxVersion();
    }

    void disable() {
        enabled.set(false);
    }

    void enable() {
        enabled.set(true);
    }

    boolean isEnabled() {
        return enabled.get();
    }

    Optional<CeFelloReplicaRecord> read(String key) throws IOException {
        ensureEnabled();
        Optional<CeFelloReplicaRecord> record = dao.read(key);
        totalReadCount.incrementAndGet();
        return record;
    }

    void write(String key, CeFelloReplicaRecord record) throws IOException {
        ensureEnabled();
        CeFelloReplicaFileDao.WriteMetadata metadata = dao.write(key, record);
        storedBytes.addAndGet(metadata.currentBytes() - metadata.previousBytes());
        if (!metadata.existed()) {
            storedKeyCount.incrementAndGet();
        }
        if (record.tombstone()) {
            totalDeleteCount.incrementAndGet();
        } else {
            totalWriteCount.incrementAndGet();
        }
    }

    long keyCount() {
        return storedKeyCount.get();
    }

    long bytesOnDisk() {
        return storedBytes.get();
    }

    long readCount() {
        return totalReadCount.get();
    }

    long writeCount() {
        return totalWriteCount.get();
    }

    long deleteCount() {
        return totalDeleteCount.get();
    }

    void close() {
        dao.close();
    }

    private void ensureEnabled() throws IOException {
        if (!enabled.get()) {
            throw new IOException("Replica " + replicaId + " is disabled");
        }
    }
}
