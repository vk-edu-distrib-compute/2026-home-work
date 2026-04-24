package company.vk.edu.distrib.compute.ce_fello;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

final class CeFelloReplicaNode {
    private final int id;
    private final CeFelloReplicaFileDao dao;
    private final AtomicBoolean enabled = new AtomicBoolean(true);
    private final AtomicLong keyCount = new AtomicLong();
    private final AtomicLong bytesOnDisk = new AtomicLong();
    private final AtomicLong readCount = new AtomicLong();
    private final AtomicLong writeCount = new AtomicLong();
    private final AtomicLong deleteCount = new AtomicLong();

    CeFelloReplicaNode(int id, Path rootDirectory) throws IOException {
        this.id = id;
        this.dao = new CeFelloReplicaFileDao(Objects.requireNonNull(rootDirectory, "rootDirectory"));

        CeFelloReplicaFileDao.ScanMetadata metadata = dao.scan();
        this.keyCount.set(metadata.keyCount());
        this.bytesOnDisk.set(metadata.totalBytes());
    }

    int id() {
        return id;
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
        readCount.incrementAndGet();
        return record;
    }

    void write(String key, CeFelloReplicaRecord record) throws IOException {
        ensureEnabled();
        CeFelloReplicaFileDao.WriteMetadata metadata = dao.write(key, record);
        bytesOnDisk.addAndGet(metadata.currentBytes() - metadata.previousBytes());
        if (!metadata.existed()) {
            keyCount.incrementAndGet();
        }
        if (record.tombstone()) {
            deleteCount.incrementAndGet();
        } else {
            writeCount.incrementAndGet();
        }
    }

    long keyCount() {
        return keyCount.get();
    }

    long bytesOnDisk() {
        return bytesOnDisk.get();
    }

    long readCount() {
        return readCount.get();
    }

    long writeCount() {
        return writeCount.get();
    }

    long deleteCount() {
        return deleteCount.get();
    }

    void close() {
        dao.close();
    }

    private void ensureEnabled() throws IOException {
        if (!enabled.get()) {
            throw new IOException("Replica " + id + " is disabled");
        }
    }
}
