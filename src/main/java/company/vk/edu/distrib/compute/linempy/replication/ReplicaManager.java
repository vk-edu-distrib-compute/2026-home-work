package company.vk.edu.distrib.compute.linempy.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Управление репликами: запись, чтение, удаление, синхронизация.
 *
 * @author Linempy
 * @since 24.04.2026
 */
public class ReplicaManager {
    private static final Logger log = LoggerFactory.getLogger(ReplicaManager.class);
    private static final String ACK_ERROR = "ack > factor";
    private static final String REPLICA_DISABLED_WARN = "Not enough replicas: available={}, required={}";

    private final ReplicaStorage storage;
    private final ReplicationConfig config;
    private final ReplicaSelector selector;
    private final Map<Integer, Boolean> replicaEnabled;
    private final Map<String, Boolean> keyExists;
    private final ReplicaStats stats;
    private final AsyncReplicaWriter asyncWriter;

    public ReplicaManager(ReplicationConfig config, int port) throws IOException {
        this.config = config;
        this.storage = new ReplicaStorage(config, port);
        this.selector = new ReplicaSelector(config.getFactor());
        this.replicaEnabled = new ConcurrentHashMap<>();
        this.keyExists = new ConcurrentHashMap<>();
        this.stats = new ReplicaStats(config.getFactor());
        this.asyncWriter = new AsyncReplicaWriter(storage, stats, config.getFactor(), config.isAsyncMode());

        for (int i = 0; i < config.getFactor(); i++) {
            replicaEnabled.put(i, true);
        }
        if (log.isInfoEnabled()) {
            log.info("ReplicaManager started: factor={}, async={}", config.getFactor(), config.isAsyncMode());
        }
    }

    public List<Integer> getReplicaIndexes(String key) {
        return selector.getReplicaIndexes(key);
    }

    public void disableReplica(int nodeId) {
        replicaEnabled.put(nodeId, false);
    }

    public void enableReplica(int nodeId) {
        replicaEnabled.put(nodeId, true);
    }

    public boolean isReplicaEnabled(int nodeId) {
        return replicaEnabled.getOrDefault(nodeId, true);
    }

    public int writeWithAck(String key, byte[] value, int requiredAck) {
        if (requiredAck > config.getFactor()) {
            throw new IllegalArgumentException(ACK_ERROR);
        }
        List<Integer> indexes = getReplicaIndexes(key);

        int available = (int) indexes.stream().filter(this::isReplicaEnabled).count();

        if (available < requiredAck) {
            if (log.isWarnEnabled()) {
                log.warn(REPLICA_DISABLED_WARN, available, requiredAck);
            }
            return 0;
        }

        int success = asyncWriter.write(key, value, requiredAck, indexes, this::isReplicaEnabled);

        if (success >= requiredAck) {
            keyExists.put(key, true);
        }
        return success;
    }

    public byte[] readAnyReplica(String key) {
        for (int idx : getReplicaIndexes(key)) {
            if (!isReplicaEnabled(idx)) {
                continue;
            }
            if (!storage.exists(key, idx)) {
                continue;
            }
            try {
                byte[] value = storage.read(key, idx);
                stats.recordRead(idx);
                return value;
            } catch (IOException e) {
                log.warn("Read failed idx={}", idx);
            }
        }
        return new byte[0];
    }

    public ReadResult readWithAck(String key, int requiredAck) {
        List<Integer> indexes = getReplicaIndexes(key);
        int responded = 0;
        byte[] best = null;

        int available = (int) indexes.stream().filter(this::isReplicaEnabled).count();
        if (available < requiredAck) {
            if (log.isWarnEnabled()) {
                log.warn(REPLICA_DISABLED_WARN, available, requiredAck);
            }
            return new ReadResult(false, null, 0);
        }

        for (int idx : indexes) {
            if (!isReplicaEnabled(idx)) {
                continue;
            }
            if (!storage.exists(key, idx)) {
                responded++;
                continue;
            }
            try {
                byte[] value = storage.read(key, idx);
                responded++;
                if (best == null) {
                    best = value;
                    stats.recordRead(idx);
                }
            } catch (IOException e) {
                log.warn("Read failed idx={}", idx);
                responded++;
            }
        }
        return new ReadResult(responded >= requiredAck, best, responded);
    }

    public int getAvailableReplicasCount(String key) {
        int count = 0;
        for (int idx : getReplicaIndexes(key)) {
            if (isReplicaEnabled(idx)) {
                count++;
            }
        }
        return count;
    }

    public int deleteAllReplicas(String key) {
        int deleted = 0;
        for (int idx : getReplicaIndexes(key)) {
            try {
                if (storage.delete(key, idx)) {
                    deleted++;
                }
            } catch (IOException e) {
                if (log.isWarnEnabled()) {
                    log.warn("Delete failed idx={}: {}", idx, e.getMessage());
                }
            }
        }
        keyExists.remove(key);
        return deleted;
    }

    public void syncReplica(int nodeId) {
        if (!isReplicaEnabled(nodeId)) {
            return;
        }
        for (Map.Entry<String, Boolean> e : keyExists.entrySet()) {
            String key = e.getKey();
            if (storage.exists(key, nodeId)) {
                continue;
            }
            byte[] val = readAnyReplica(key);
            if (val != null) {
                try {
                    storage.write(key, nodeId, val);
                    if (log.isDebugEnabled()) {
                        log.debug("Synced key {} to replica {}", key, nodeId);
                    }
                } catch (IOException ex) {
                    log.warn("Sync failed key={}", key);
                }
            }
        }
    }

    public void close() {
        asyncWriter.close();
    }

    public long getKeyCount(int replicaId) throws IOException {
        return storage.getKeyCountForReplica(replicaId);
    }

    public long getReadCount(int replicaId) {
        return stats.getReadCount(replicaId);
    }

    public long getWriteCount(int replicaId) {
        return stats.getWriteCount(replicaId);
    }
}

