package company.vk.edu.distrib.compute.nihuaway00.replication;

import company.vk.edu.distrib.compute.nihuaway00.storage.EntityDao;
import company.vk.edu.distrib.compute.nihuaway00.storage.VersionedEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
        if (ack > numberOfReplicas()) {
            throw new IllegalArgumentException("ack is bigger than enabled replicas count");
        }
    }

    public int numberOfReplicas() {
        return replicas.size();
    }

    public void disableReplica(int nodeId) {
        replicas.get(nodeId).disable();
    }

    public void enableReplica(int nodeId) {
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

        List<VersionedEntry> responses = readFromReplicas(key, enabledReplicas);
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

        List<Boolean> responses = writeToReplicas(key, data, enabledReplicas);
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

        List<Boolean> responses = deleteToReplicas(key, enabledReplicas);
        if (responses.size() < ack) {
            if (log.isWarnEnabled()) {
                log.warn("Partial delete for key={}: {}/{} replicas confirmed, ack={}",
                        key, responses.size(), enabledReplicas.size(), ack);
            }
            throw new InsufficientReplicasException();
        }
    }

    private List<VersionedEntry> readFromReplicas(String key, List<ReplicaNode> replicas) {
        List<CompletableFuture<VersionedEntry>> futures = replicas.stream()
                .map(r -> CompletableFuture.supplyAsync(() -> {
                    EntityDao dao = r.dao();
                    try (dao) {
                        VersionedEntry versioned = dao.getVersioned(key);
                        return versioned != null ? versioned : VersionedEntry.absent();
                    } catch (IOException e) {
                        if (log.isWarnEnabled()) {
                            log.warn("Replica {} failed GET {}: {}", r.nodeId(), key, e.getMessage());
                        }
                        return null;
                    }
                }, EXECUTOR))
                .toList();

        return futures.stream()
                .map(f -> {
                    try {
                        return f.get(1, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .toList();
    }

    private List<Boolean> writeToReplicas(String key, byte[] value, List<ReplicaNode> replicas) {
        List<CompletableFuture<Boolean>> futures = replicas.stream()
                .map(r -> CompletableFuture.supplyAsync(() -> {
                    try {
                        r.dao().upsert(key, value);
                        return true;
                    } catch (IOException e) {
                        if (log.isWarnEnabled()) {
                            log.warn("Replica {} failed PUT {}: {}", r.nodeId(), key, e.getMessage());
                        }
                        return null;
                    }
                }, EXECUTOR))
                .toList();

        return futures.stream()
                .map(f -> {
                    try {
                        return f.get(1, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .toList();
    }

    private List<Boolean> deleteToReplicas(String key, List<ReplicaNode> replicas) {
        List<CompletableFuture<Boolean>> futures = replicas.stream()
                .map(r -> CompletableFuture.supplyAsync(() -> {
                    try {
                        r.dao().delete(key);
                        return true;
                    } catch (IOException e) {
                        if (log.isWarnEnabled()) {
                            log.warn("Replica {} failed DELETE {}: {}", r.nodeId(), key, e.getMessage());
                        }
                        return null;
                    }
                }, EXECUTOR))
                .toList();

        return futures.stream()
                .map(f -> {
                    try {
                        return f.get(1, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .toList();
    }
}
