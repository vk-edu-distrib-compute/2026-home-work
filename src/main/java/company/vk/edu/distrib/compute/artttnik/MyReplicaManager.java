package company.vk.edu.distrib.compute.artttnik;

import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

final class MyReplicaManager implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MyReplicaManager.class);
    private static final int RECORD_HEADER_SIZE = Long.BYTES + 1 + Integer.BYTES;

    private final List<ReplicaNode> replicas;
    private final AtomicLong versionGenerator;
    private final ReentrantLock lock = new ReentrantLock();

    MyReplicaManager(List<Dao<byte[]>> replicaDaos) {
        if (replicaDaos == null || replicaDaos.isEmpty()) {
            throw new IllegalArgumentException("At least one replica storage is required");
        }

        List<ReplicaNode> result = new ArrayList<>(replicaDaos.size());
        for (Dao<byte[]> replicaDao : replicaDaos) {
            result.add(new ReplicaNode(replicaDao));
        }

        this.replicas = List.copyOf(result);
        this.versionGenerator = new AtomicLong(System.nanoTime());
    }

    int numberOfReplicas() {
        return replicas.size();
    }

    void disableReplica(int nodeId) {
        lock.lock();
        try {
            getReplica(nodeId).setEnabled(false);
        } finally {
            lock.unlock();
        }
    }

    void enableReplica(int nodeId) {
        lock.lock();
        try {
            getReplica(nodeId).setEnabled(true);
        } finally {
            lock.unlock();
        }
    }

    int put(String key, byte[] value) {
        return write(key, ReplicaRecord.value(nextVersion(), value));
    }

    int delete(String key) {
        return write(key, ReplicaRecord.deleted(nextVersion()));
    }

    ReplicaReadResult readLatest(String key) {
        lock.lock();
        try {
            int confirmations = 0;
            ReplicaRecord latest = null;

            for (ReplicaNode replica : replicas) {
                if (!replica.isEnabled()) {
                    continue;
                }

                try {
                    ReplicaRecord record = replica.read(key);
                    confirmations++;
                    if (record != null && (latest == null || record.version() > latest.version())) {
                        latest = record;
                    }
                } catch (IOException e) {
                    if (log.isDebugEnabled()) {
                        log.debug("Failed to read from replica", e);
                    }
                }
            }

            return new ReplicaReadResult(confirmations, latest);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            for (ReplicaNode replica : replicas) {
                replica.close();
            }
        } finally {
            lock.unlock();
        }
    }

    private int write(String key, ReplicaRecord record) {
        lock.lock();
        try {
            int confirmations = 0;
            for (ReplicaNode replica : replicas) {
                if (!replica.isEnabled()) {
                    continue;
                }

                try {
                    replica.write(key, record);
                    confirmations++;
                } catch (IOException e) {
                    if (log.isDebugEnabled()) {
                        log.debug("Failed to write to replica", e);
                    }
                }
            }
            return confirmations;
        } finally {
            lock.unlock();
        }
    }

    private long nextVersion() {
        return versionGenerator.incrementAndGet();
    }

    private ReplicaNode getReplica(int nodeId) {
        if (nodeId < 0 || nodeId >= replicas.size()) {
            throw new IllegalArgumentException("Replica id out of range: " + nodeId);
        }
        return replicas.get(nodeId);
    }

    record ReplicaReadResult(int confirmations, ReplicaRecord latest) {
    }

    record ReplicaRecord(long version, boolean deleted, byte[] value) {
        private static ReplicaRecord value(long version, byte[] value) {
            return new ReplicaRecord(version, false, value == null ? new byte[0] : value);
        }

        private static ReplicaRecord deleted(long version) {
            return new ReplicaRecord(version, true, new byte[0]);
        }

        private byte[] encode() {
            byte[] payload = value == null ? new byte[0] : value;
            ByteBuffer buffer = ByteBuffer.allocate(RECORD_HEADER_SIZE + payload.length);
            buffer.putLong(version);
            buffer.put((byte) (deleted ? 1 : 0));
            buffer.putInt(payload.length);
            buffer.put(payload);
            return buffer.array();
        }

        private static ReplicaRecord decode(byte[] bytes) {
            if (bytes == null || bytes.length < RECORD_HEADER_SIZE) {
                return null;
            }

            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            long version = buffer.getLong();
            boolean deleted = buffer.get() != 0;
            int length = buffer.getInt();

            if (length < 0 || length > buffer.remaining()) {
                return null;
            }

            byte[] value = new byte[length];
            buffer.get(value);
            return new ReplicaRecord(version, deleted, value);
        }
    }

    private static final class ReplicaNode {
        private final Dao<byte[]> dao;
        private boolean enabled = true;

        private ReplicaNode(Dao<byte[]> dao) {
            this.dao = dao;
        }

        private ReplicaRecord read(String key) throws IOException {
            try {
                return ReplicaRecord.decode(dao.get(key));
            } catch (NoSuchElementException e) {
                return null;
            }
        }

        private void write(String key, ReplicaRecord record) throws IOException {
            dao.upsert(key, record.encode());
        }

        private void close() throws IOException {
            dao.close();
        }

        private boolean isEnabled() {
            return enabled;
        }

        private void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }
}
