package company.vk.edu.distrib.compute.usl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

final class ReplicaCoordinator implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ReplicaCoordinator.class);

    private final ReplicaCluster cluster;
    private final ExecutorService ioExecutor;
    private final AtomicLong versionSequence = new AtomicLong();

    ReplicaCoordinator(int replicaCount, Path storageRoot) throws IOException {
        this.cluster = new ReplicaCluster(replicaCount, storageRoot);
        this.ioExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    int replicaCount() {
        return cluster.replicaCount();
    }

    void validateAck(int ack) {
        cluster.validateAck(ack);
    }

    void disableReplica(int nodeId) {
        cluster.replica(nodeId).disable();
    }

    void enableReplica(int nodeId) {
        cluster.replica(nodeId).enable();
    }

    ReplicaStatsSnapshot replicaStats(int nodeId) throws IOException {
        return cluster.replica(nodeId).stats();
    }

    ReplicaAccessStatsSnapshot replicaAccessStats(int nodeId) {
        return cluster.replica(nodeId).accessStats();
    }

    int upsert(String key, byte[] value) {
        return replicate(key, VersionedValue.value(nextVersion(), value));
    }

    int delete(String key) {
        return replicate(key, VersionedValue.tombstone(nextVersion()));
    }

    ReplicaReadResult read(String key, int ack) {
        List<ReplicaCluster.Replica> replicas = cluster.orderedReplicasForKey(key);
        List<CompletableFuture<ReadOutcome>> futures = new ArrayList<>(replicas.size());
        for (ReplicaCluster.Replica replica : replicas) {
            futures.add(CompletableFuture.supplyAsync(() -> readReplica(replica, key), ioExecutor));
        }

        List<ReadOutcome> outcomes = new ArrayList<>(futures.size());
        for (CompletableFuture<ReadOutcome> future : futures) {
            outcomes.add(future.join());
        }

        int successfulResponses = countSuccessfulResponses(outcomes);
        VersionedValue freshestValue = freshestValue(outcomes);
        if (successfulResponses >= ack && freshestValue != null) {
            repairReplicas(key, freshestValue, outcomes);
        }
        return new ReplicaReadResult(successfulResponses, freshestValue);
    }

    private int replicate(String key, VersionedValue value) {
        List<ReplicaCluster.Replica> replicas = cluster.orderedReplicasForKey(key);
        List<CompletableFuture<Boolean>> futures = new ArrayList<>(replicas.size());
        for (ReplicaCluster.Replica replica : replicas) {
            futures.add(CompletableFuture.supplyAsync(
                () -> writeReplica(replica, key, value, true),
                ioExecutor
            ));
        }

        int successfulWrites = 0;
        for (CompletableFuture<Boolean> future : futures) {
            if (future.join()) {
                successfulWrites++;
            }
        }
        return successfulWrites;
    }

    private void repairReplicas(String key, VersionedValue value, List<ReadOutcome> outcomes) {
        List<CompletableFuture<Boolean>> repairFutures = new ArrayList<>(outcomes.size());
        for (ReadOutcome outcome : outcomes) {
            if (outcome.successful()) {
                repairFutures.add(CompletableFuture.supplyAsync(
                    () -> writeReplica(outcome.replica(), key, value, false),
                    ioExecutor
                ));
            }
        }
        for (CompletableFuture<Boolean> repairFuture : repairFutures) {
            repairFuture.join();
        }
    }

    private ReadOutcome readReplica(ReplicaCluster.Replica replica, String key) {
        if (!replica.isEnabled()) {
            return ReadOutcome.failure(replica);
        }

        try {
            return ReadOutcome.success(replica, replica.read(key));
        } catch (IOException e) {
            log.warn("Read failed for key={} on replica {}", key, replica.id(), e);
            return ReadOutcome.failure(replica);
        }
    }

    private boolean writeReplica(
        ReplicaCluster.Replica replica,
        String key,
        VersionedValue value,
        boolean countAccess
    ) {
        if (!replica.isEnabled()) {
            return false;
        }

        try {
            return replica.writeIfNewer(key, value, countAccess);
        } catch (IOException e) {
            log.warn("Write failed for key={} on replica {}", key, replica.id(), e);
            return false;
        }
    }

    private static int countSuccessfulResponses(List<ReadOutcome> outcomes) {
        int successfulResponses = 0;
        for (ReadOutcome outcome : outcomes) {
            if (outcome.successful()) {
                successfulResponses++;
            }
        }
        return successfulResponses;
    }

    private static VersionedValue freshestValue(List<ReadOutcome> outcomes) {
        VersionedValue freshestValue = null;
        for (ReadOutcome outcome : outcomes) {
            if (!outcome.successful() || outcome.value() == null) {
                continue;
            }
            if (VersionedValue.isNewer(outcome.value(), freshestValue)) {
                freshestValue = outcome.value();
            }
        }
        return freshestValue;
    }

    private long nextVersion() {
        return versionSequence.incrementAndGet();
    }

    @Override
    public void close() {
        ioExecutor.shutdown();
        try {
            if (!ioExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                ioExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            ioExecutor.shutdownNow();
        }
    }

    private record ReadOutcome(ReplicaCluster.Replica replica, VersionedValue value, boolean successful) {
        private static ReadOutcome success(ReplicaCluster.Replica replica, VersionedValue value) {
            return new ReadOutcome(replica, value, true);
        }

        private static ReadOutcome failure(ReplicaCluster.Replica replica) {
            return new ReadOutcome(replica, null, false);
        }
    }
}
