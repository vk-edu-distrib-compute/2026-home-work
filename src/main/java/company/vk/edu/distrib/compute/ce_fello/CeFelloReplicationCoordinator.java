package company.vk.edu.distrib.compute.ce_fello;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

final class CeFelloReplicationCoordinator {
    private static final Logger log = LoggerFactory.getLogger(CeFelloReplicationCoordinator.class);
    private static final int REPLICA_TIMEOUT_SECONDS = 1;

    private final List<CeFelloReplicaNode> replicaNodes;
    private final int replicationFactor;
    private final CeFelloReplicaPicker replicaPicker;
    private final Executor executor;
    private final AtomicLong versionGenerator;

    CeFelloReplicationCoordinator(
            List<CeFelloReplicaNode> replicaNodes,
            int replicationFactor,
            CeFelloReplicaPicker replicaPicker,
            Executor executor,
            long initialVersion
    ) {
        this.replicaNodes = List.copyOf(replicaNodes);
        this.replicationFactor = replicationFactor;
        this.replicaPicker = replicaPicker;
        this.executor = executor;
        this.versionGenerator = new AtomicLong(initialVersion);
    }

    void upsert(String key, byte[] value, int ack) {
        executeWrite(key, CeFelloReplicaRecord.live(nextVersion(), value), ack);
    }

    void delete(String key, int ack) {
        executeWrite(key, CeFelloReplicaRecord.tombstone(nextVersion()), ack);
    }

    ReadResponse get(String key, int ack) {
        List<CeFelloReplicaNode> selectedReplicas = pickReplicas(key);
        List<ReplicaReadResult> results = selectedReplicas.stream()
                .map(replica -> readAsync(replica, key))
                .map(CompletableFuture::join)
                .toList();

        long confirmations = results.stream()
                .filter(ReplicaReadResult::acknowledged)
                .count();
        ensureAck(confirmations, ack, key, "read");

        Optional<CeFelloReplicaRecord> freshest = results.stream()
                .filter(ReplicaReadResult::acknowledged)
                .map(ReplicaReadResult::record)
                .flatMap(Optional::stream)
                .max(Comparator.comparingLong(CeFelloReplicaRecord::version));

        if (freshest.isEmpty()) {
            return ReadResponse.notFound();
        }

        CeFelloReplicaRecord record = freshest.get();
        repairStaleReplicas(key, record, results);
        return record.tombstone()
                ? ReadResponse.notFound()
                : ReadResponse.found(record.value());
    }

    private void executeWrite(String key, CeFelloReplicaRecord record, int ack) {
        List<CeFelloReplicaNode> selectedReplicas = pickReplicas(key);
        List<CompletableFuture<Boolean>> futures = selectedReplicas.stream()
                .map(replica -> writeAsync(replica, key, record))
                .toList();

        long confirmations = futures.stream()
                .map(CompletableFuture::join)
                .filter(Boolean::booleanValue)
                .count();
        ensureAck(confirmations, ack, key, record.tombstone() ? "delete" : "write");
    }

    private void repairStaleReplicas(String key, CeFelloReplicaRecord freshest, List<ReplicaReadResult> results) {
        for (ReplicaReadResult result : results) {
            if (!result.acknowledged()) {
                continue;
            }

            boolean needsRepair = result.record().isEmpty()
                    || result.record().get().version() < freshest.version();
            if (!needsRepair) {
                continue;
            }

            CompletableFuture.runAsync(() -> {
                try {
                    result.replica().write(key, freshest);
                } catch (IOException e) {
                    log.warn("Failed to repair replica {}", result.replica().id(), e);
                }
            }, executor);
        }
    }

    private CompletableFuture<ReplicaReadResult> readAsync(CeFelloReplicaNode replica, String key) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return new ReplicaReadResult(replica, true, replica.read(key));
            } catch (IOException e) {
                log.info("Replica {} is unavailable for read key={}", replica.id(), key);
                return new ReplicaReadResult(replica, false, Optional.empty());
            }
        }, executor).completeOnTimeout(
                new ReplicaReadResult(replica, false, Optional.empty()),
                REPLICA_TIMEOUT_SECONDS,
                TimeUnit.SECONDS
        );
    }

    private CompletableFuture<Boolean> writeAsync(CeFelloReplicaNode replica, String key, CeFelloReplicaRecord record) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                replica.write(key, record);
                return true;
            } catch (IOException e) {
                log.info("Replica {} is unavailable for write key={}", replica.id(), key);
                return false;
            }
        }, executor).completeOnTimeout(false, REPLICA_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private List<CeFelloReplicaNode> pickReplicas(String key) {
        List<Integer> replicaIds = replicaPicker.pick(key, replicationFactor);
        List<CeFelloReplicaNode> result = new ArrayList<>(replicaIds.size());
        for (Integer replicaId : replicaIds) {
            result.add(replicaNodes.get(replicaId));
        }
        return List.copyOf(result);
    }

    private long nextVersion() {
        return versionGenerator.incrementAndGet();
    }

    private static void ensureAck(long confirmations, int ack, String key, String operation) {
        if (confirmations < ack) {
            throw new CeFelloInsufficientAckException(operation + " failed for key=" + key + ", ack=" + ack);
        }
    }

    record ReadResponse(boolean found, byte[] body) {
        static ReadResponse found(byte[] body) {
            return new ReadResponse(true, body);
        }

        static ReadResponse notFound() {
            return new ReadResponse(false, new byte[0]);
        }
    }

    private record ReplicaReadResult(
            CeFelloReplicaNode replica,
            boolean acknowledged,
            Optional<CeFelloReplicaRecord> record
    ) {
    }
}
