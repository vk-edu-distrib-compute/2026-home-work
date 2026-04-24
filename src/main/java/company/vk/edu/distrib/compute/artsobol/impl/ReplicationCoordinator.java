package company.vk.edu.distrib.compute.artsobol.impl;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

final class ReplicationCoordinator implements AutoCloseable {
    private final ReplicaCluster cluster;
    private final ReplicaCommandExecutor executor = new ReplicaCommandExecutor();
    private final AtomicLong versionGenerator = new AtomicLong();

    ReplicationCoordinator(int replicaCount, Path storageRoot) {
        this.cluster = new ReplicaCluster(replicaCount, storageRoot);
    }

    int replicaCount() {
        return cluster.replicaCount();
    }

    void validateAck(int ack) {
        cluster.validateAck(ack);
    }

    void disableReplica(int nodeId) {
        cluster.disableReplica(nodeId);
    }

    void enableReplica(int nodeId) {
        cluster.enableReplica(nodeId);
    }

    ReplicaStats replicaStats(int nodeId) {
        return cluster.replicaStats(nodeId);
    }

    ReplicaAccessStats replicaAccessStats(int nodeId) {
        return cluster.replicaAccessStats(nodeId);
    }

    int put(String key, byte[] body) {
        return executor.replicate(key, VersionedEntry.value(nextVersion(), body), cluster.replicasForKey(key));
    }

    int delete(String key) {
        return executor.replicate(key, VersionedEntry.tombstone(nextVersion()), cluster.replicasForKey(key));
    }

    ReadResult read(String key, int ack) {
        return executor.read(key, ack, cluster.replicasForKey(key));
    }

    private long nextVersion() {
        return versionGenerator.incrementAndGet();
    }

    @Override
    public void close() {
        executor.close();
    }
}
