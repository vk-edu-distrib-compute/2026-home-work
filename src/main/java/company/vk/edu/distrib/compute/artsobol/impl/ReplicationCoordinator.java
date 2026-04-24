package company.vk.edu.distrib.compute.artsobol.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.nio.file.Path;

final class ReplicationCoordinator implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ReplicationCoordinator.class);
    private static final int MIN_REPLICA_COUNT = 3;
    private static final long REPLICA_TIMEOUT_MS = 200L;
    private static final long HASH_OFFSET_BASIS = 0xcbf29ce484222325L;
    private static final long HASH_PRIME = 0x100000001b3L;

    private final List<Replica> replicas;
    private final AtomicLong versionGenerator = new AtomicLong();
    private final ExecutorService ioExecutor = Executors.newVirtualThreadPerTaskExecutor();

    ReplicationCoordinator(int replicaCount, Path storageRoot) {
        if (replicaCount < MIN_REPLICA_COUNT) {
            throw new IllegalArgumentException("replicaCount must be at least " + MIN_REPLICA_COUNT);
        }
        this.replicas = new ArrayList<>(replicaCount);
        for (int i = 0; i < replicaCount; i++) {
            replicas.add(new Replica(i, storageRoot.resolve("replica-" + i)));
        }
    }

    int replicaCount() {
        return replicas.size();
    }

    void validateAck(int ack) {
        if (ack <= 0 || ack > replicaCount()) {
            throw new IllegalArgumentException("ack is out of range");
        }
    }

    void disableReplica(int nodeId) {
        replica(nodeId).disable();
    }

    void enableReplica(int nodeId) {
        replica(nodeId).enable();
    }

    ReplicaStats replicaStats(int nodeId) {
        return replica(nodeId).stats(nodeId);
    }

    ReplicaAccessStats replicaAccessStats(int nodeId) {
        return replica(nodeId).accessStats(nodeId);
    }

    int put(String key, byte[] body) {
        return replicate(key, VersionedEntry.value(versionGenerator.incrementAndGet(), body));
    }

    int delete(String key) {
        return replicate(key, VersionedEntry.tombstone(versionGenerator.incrementAndGet()));
    }

    ReadResult read(String key, int ack) {
        List<Replica> orderedReplicas = orderedReplicasForKey(key);
        log.debug("Replica read started: key={}, ack={}, replicas={}", key, ack, replicaIds(orderedReplicas));

        List<CompletableFuture<ReadOutcome>> futures = new ArrayList<>(orderedReplicas.size());
        for (Replica replica : orderedReplicas) {
            if (replica.isEnabled()) {
                futures.add(readFuture(replica, key));
            }
        }

        int successfulResponses = 0;
        VersionedEntry freshest = null;
        List<Replica> responsiveReplicas = new ArrayList<>(futures.size());
        for (CompletableFuture<ReadOutcome> future : futures) {
            ReadOutcome outcome = future.join();
            if (!outcome.success()) {
                continue;
            }
            successfulResponses++;
            responsiveReplicas.add(outcome.replica());
            VersionedEntry current = outcome.entry();
            if (current != null && isNewer(current, freshest)) {
                freshest = current;
            }
        }

        if (successfulResponses >= ack && freshest != null) {
            repair(key, freshest, responsiveReplicas);
            log.debug("Replica read finished: key={}, ack={}, successes={}", key, ack, successfulResponses);
        } else if (successfulResponses < ack) {
            log.warn(
                    "Replica read quorum not reached: key={}, ack={}, successes={}",
                    key,
                    ack,
                    successfulResponses
            );
        }

        return new ReadResult(successfulResponses, freshest);
    }

    private int replicate(String key, VersionedEntry entry) {
        List<Replica> orderedReplicas = orderedReplicasForKey(key);
        String operation = entry.tombstone() ? "delete" : "put";
        log.debug("Replica {} started: key={}, replicas={}", operation, key, replicaIds(orderedReplicas));

        List<CompletableFuture<Boolean>> futures = new ArrayList<>(orderedReplicas.size());
        for (Replica replica : orderedReplicas) {
            if (replica.isEnabled()) {
                futures.add(writeFuture(replica, key, entry, true));
            }
        }

        int successfulWrites = 0;
        for (CompletableFuture<Boolean> future : futures) {
            if (future.join()) {
                successfulWrites++;
            }
        }

        log.debug("Replica {} finished: key={}, successes={}", operation, key, successfulWrites);
        return successfulWrites;
    }

    private void repair(String key, VersionedEntry freshest, List<Replica> responsiveReplicas) {
        List<CompletableFuture<Boolean>> repairs = new ArrayList<>(responsiveReplicas.size());
        for (Replica replica : responsiveReplicas) {
            repairs.add(writeFuture(replica, key, freshest, false));
        }
        repairs.forEach(CompletableFuture::join);
    }

    private Replica replica(int nodeId) {
        if (nodeId < 0 || nodeId >= replicas.size()) {
            throw new IllegalArgumentException("Replica id out of range: " + nodeId);
        }
        return replicas.get(nodeId);
    }

    private static boolean isNewer(VersionedEntry candidate, VersionedEntry current) {
        if (current == null) {
            return true;
        }
        if (candidate.version() != current.version()) {
            return candidate.version() > current.version();
        }
        return candidate.tombstone() && !current.tombstone();
    }

    private CompletableFuture<ReadOutcome> readFuture(Replica replica, String key) {
        return CompletableFuture
                .supplyAsync(() -> replica.readAsync(key), ioExecutor)
                .orTimeout(REPLICA_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .exceptionally(error -> {
                    log.warn(
                            "Replica read failed: key={}, replicaId={}, reason={}",
                            key,
                            replica.id(),
                            error.toString()
                    );
                    return new ReadOutcome(replica, null, false);
                });
    }

    private CompletableFuture<Boolean> writeFuture(
            Replica replica,
            String key,
            VersionedEntry entry,
            boolean countAccess
    ) {
        String operation = entry.tombstone() ? "delete" : "put";
        return CompletableFuture
                .supplyAsync(() -> replica.writeAsync(key, entry, countAccess), ioExecutor)
                .orTimeout(REPLICA_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .exceptionally(error -> {
                    log.warn(
                            "Replica {} failed: key={}, replicaId={}, reason={}",
                            operation,
                            key,
                            replica.id(),
                            error.toString()
                    );
                    return false;
                });
    }

    private List<Replica> orderedReplicasForKey(String key) {
        List<Replica> ordered = new ArrayList<>(replicas);
        ordered.sort((left, right) -> Long.compareUnsigned(
                rendezvousScore(key, right.id()),
                rendezvousScore(key, left.id())
        ));
        return ordered;
    }

    private static long rendezvousScore(String key, int replicaId) {
        long hash = HASH_OFFSET_BASIS;
        for (int i = 0; i < key.length(); i++) {
            hash ^= key.charAt(i);
            hash *= HASH_PRIME;
        }
        hash ^= '#';
        hash *= HASH_PRIME;
        hash ^= replicaId;
        hash *= HASH_PRIME;
        return mix64(hash);
    }

    private static long mix64(long value) {
        value ^= value >>> 33;
        value *= 0xff51afd7ed558ccdL;
        value ^= value >>> 33;
        value *= 0xc4ceb9fe1a85ec53L;
        value ^= value >>> 33;
        return value;
    }

    private static String replicaIds(List<Replica> orderedReplicas) {
        List<Integer> ids = new ArrayList<>(orderedReplicas.size());
        for (Replica replica : orderedReplicas) {
            ids.add(replica.id());
        }
        return ids.toString();
    }

    record ReadResult(int successfulResponses, VersionedEntry entry) {
    }

    record VersionedEntry(long version, boolean tombstone, byte[] value) {
        VersionedEntry {
            value = value == null ? null : Arrays.copyOf(value, value.length);
        }

        static VersionedEntry value(long version, byte[] value) {
            return new VersionedEntry(version, false, value);
        }

        static VersionedEntry tombstone(long version) {
            return new VersionedEntry(version, true, null);
        }

        byte[] body() {
            return value == null ? null : Arrays.copyOf(value, value.length);
        }
    }

    record ReplicaStats(
            int replicaId,
            boolean enabled,
            int totalKeys,
            int liveKeys,
            int tombstones,
            long bytes
    ) {
        String toJson() {
            return """
                    {"replicaId":%d,"enabled":%s,"totalKeys":%d,"liveKeys":%d,"tombstones":%d,"bytes":%d}
                    """.formatted(replicaId, enabled, totalKeys, liveKeys, tombstones, bytes);
        }
    }

    record ReplicaAccessStats(int replicaId, long reads, long writes, long deletes) {
        String toJson() {
            return """
                    {"replicaId":%d,"reads":%d,"writes":%d,"deletes":%d}
                    """.formatted(replicaId, reads, writes, deletes);
        }
    }

    @Override
    public void close() {
        ioExecutor.close();
    }

    private static final class Replica {
        private final int id;
        private final AtomicBoolean enabled = new AtomicBoolean(true);
        private final ReplicaFileStore store;

        private Replica(int id, Path replicaPath) {
            this.id = id;
            this.store = new ReplicaFileStore(replicaPath);
        }

        private int id() {
            return id;
        }

        private boolean isEnabled() {
            return enabled.get();
        }

        private void disable() {
            enabled.set(false);
        }

        private void enable() {
            enabled.set(true);
        }

        private ReadOutcome readAsync(String key) {
            try {
                return new ReadOutcome(this, store.read(key), true);
            } catch (IOException e) {
                return new ReadOutcome(this, null, false);
            }
        }

        private boolean writeAsync(String key, VersionedEntry entry, boolean countAccess) {
            try {
                return store.writeIfNewer(key, entry, countAccess);
            } catch (IOException e) {
                return false;
            }
        }

        private ReplicaStats stats(int replicaId) {
            try {
                return store.stats(replicaId, isEnabled());
            } catch (IOException e) {
                throw new IllegalStateException("Failed to read replica stats", e);
            }
        }

        private ReplicaAccessStats accessStats(int replicaId) {
            return store.accessStats(replicaId);
        }
    }

    private record ReadOutcome(Replica replica, VersionedEntry entry, boolean success) {
    }
}
