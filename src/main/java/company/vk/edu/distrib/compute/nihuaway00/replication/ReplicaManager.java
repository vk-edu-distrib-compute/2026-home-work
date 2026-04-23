package company.vk.edu.distrib.compute.nihuaway00.replication;

import company.vk.edu.distrib.compute.nihuaway00.storage.EntityDao;
import company.vk.edu.distrib.compute.nihuaway00.storage.VersionedEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class ReplicaManager {
    private static final Logger log = LoggerFactory.getLogger(ReplicaManager.class);
    private static final ExecutorService EXECUTOR =
            Executors.newVirtualThreadPerTaskExecutor();
    private final List<ReplicaNode> replicas;
    private final ReplicaSelector replicaSelector;

    public ReplicaManager(List<ReplicaNode> replicas, ReplicaSelector replicaSelector) {
        this.replicas = replicas;
        this.replicaSelector = replicaSelector;
    }

    public boolean available() {
        return replicas.stream().anyMatch(ReplicaNode::isEnabled);
    }

    private void checkAckValue(int ack) {
        if (ack < 1 || ack > numberOfReplicas()) {
            throw new IllegalArgumentException(
                    "ack must be in range [1, " + numberOfReplicas() + "]"
            );
        }
    }

    private void validateNodeId(int nodeId) {
        if (nodeId < 0 || nodeId >= replicas.size()) {
            throw new IllegalArgumentException("Invalid replica nodeId: " + nodeId);
        }
    }

    public int numberOfReplicas() {
        return replicas.size();
    }

    public void disableReplica(int nodeId) {
        validateNodeId(nodeId);
        replicas.get(nodeId).disable();
    }

    public void enableReplica(int nodeId) {
        validateNodeId(nodeId);
        replicas.get(nodeId).enable();
    }

    private List<ReplicaNode> getEnabledReplicas(String key) {
        List<ReplicaNode> enabled = replicas.stream()
                .filter(ReplicaNode::isEnabled)
                .toList();
        return replicaSelector.select(key, enabled);
    }

    public byte[] get(String key, int ack) {
        checkAckValue(ack);
        List<ReplicaNode> enabledReplicas = getEnabledReplicas(key);

        List<VersionedEntry> responses = readFromReplicas(key, enabledReplicas, ack);
        if (responses.size() < ack) {
            throw new InsufficientReplicasException();
        }

        VersionedEntry newest = responses.stream()
                .max(Comparator.comparingLong(VersionedEntry::getTimestamp))
                .orElseThrow();
        if (newest.isTombstone()) {
            throw new NoSuchElementException();
        }
        return newest.getData();
    }

    public void put(String key, byte[] data, int ack) {
        checkAckValue(ack);
        List<ReplicaNode> enabledReplicas = getEnabledReplicas(key);

        List<Boolean> responses = writeToReplicas(key, data, enabledReplicas, ack);
        if (responses.size() < ack) {
            if (log.isWarnEnabled()) {
                log.warn("Partial write for key={}: {}/{} replicas confirmed, ack={}",
                        key, responses.size(), enabledReplicas.size(), ack);
            }
            throw new InsufficientReplicasException();
        }
    }

    public void delete(String key, int ack) {
        checkAckValue(ack);
        List<ReplicaNode> enabledReplicas = getEnabledReplicas(key);

        List<Boolean> responses = deleteToReplicas(key, enabledReplicas, ack);
        if (responses.size() < ack) {
            if (log.isWarnEnabled()) {
                log.warn("Partial delete for key={}: {}/{} replicas confirmed, ack={}",
                        key, responses.size(), enabledReplicas.size(), ack);
            }
            throw new InsufficientReplicasException();
        }
    }

    private List<VersionedEntry> readFromReplicas(String key, List<ReplicaNode> replicas, int ack) {
        return executeOnReplicas(key, replicas, ack, dao -> {
            VersionedEntry versioned = dao.getVersioned(key);
            return versioned != null ? versioned : VersionedEntry.getAbsentInstance();
        });
    }

    private List<Boolean> writeToReplicas(String key, byte[] value,
                                          List<ReplicaNode> replicas, int ack) {
        return executeOnReplicas(key, replicas, ack, dao -> {
            dao.upsert(key, value);
            return true;
        });
    }

    private List<Boolean> deleteToReplicas(String key,
                                           List<ReplicaNode> replicas, int ack) {
        return executeOnReplicas(key, replicas, ack, dao -> {
            dao.delete(key);
            return true;
        });
    }

    private <T> List<T> executeOnReplicas(
            String key,
            List<ReplicaNode> replicas,
            int ack,
            DaoOperation<T> operation
    ) {
        var ecs = new ExecutorCompletionService<T>(EXECUTOR);
        replicas.forEach(replica -> ecs.submit(
                () -> executeReplicaOperation(replica, key, operation))
        );
        return collectSuccessfulResults(ecs, replicas.size(), ack);
    }

    private <T> T executeReplicaOperation(
            ReplicaNode replica,
            String key,
            DaoOperation<T> operation
    ) {
        EntityDao dao = replica.getDao();
        try (dao) {
            return operation.execute(dao);
        } catch (IOException e) {
            if (log.isWarnEnabled()) {
                log.warn("Replica {} operation failed for key={}: {}",
                        replica.getNodeId(), key, e.getMessage());
            }
            return null;
        }
    }

    private <T> List<T> collectSuccessfulResults(
            ExecutorCompletionService<T> ecs,
            int replicasCount,
            int ack
    ) {
        List<T> results = new ArrayList<>();
        for (int i = 0; i < replicasCount && results.size() < ack; i++) {
            try {
                Future<T> future = ecs.poll(1, TimeUnit.SECONDS);
                if (future == null) {
                    break;
                }
                T value = future.get();
                if (value != null) {
                    results.add(value);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                if (log.isWarnEnabled()) {
                    log.warn("Unexpected error while polling replica result: {}",
                            e.getMessage());
                }
            }
        }
        return results;
    }

    @FunctionalInterface
    private interface DaoOperation<T> {
        T execute(EntityDao dao) throws IOException;
    }
}
