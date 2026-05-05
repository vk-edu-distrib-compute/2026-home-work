package company.vk.edu.distrib.compute.artsobol.impl;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

final class ReplicaCluster {
    private static final int MIN_REPLICA_COUNT = 3;
    private static final long HASH_OFFSET_BASIS = 0xcbf29ce484222325L;
    private static final long HASH_PRIME = 0x100000001b3L;

    private final List<Replica> replicas;

    ReplicaCluster(int replicaCount, Path storageRoot) {
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
        return replica(nodeId).stats();
    }

    ReplicaAccessStats replicaAccessStats(int nodeId) {
        return replica(nodeId).accessStats();
    }

    List<Replica> replicasForKey(String key) {
        List<Replica> ordered = new ArrayList<>(replicas);
        ordered.sort((left, right) -> Long.compareUnsigned(
                rendezvousScore(key, right.id()),
                rendezvousScore(key, left.id())
        ));
        List<Replica> enabledReplicas = new ArrayList<>(ordered.size());
        for (Replica replica : ordered) {
            if (replica.isEnabled()) {
                enabledReplicas.add(replica);
            }
        }
        return enabledReplicas;
    }

    private Replica replica(int nodeId) {
        if (nodeId < 0 || nodeId >= replicas.size()) {
            throw new IllegalArgumentException("Replica id out of range: " + nodeId);
        }
        return replicas.get(nodeId);
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
        long mixed = value;
        mixed ^= mixed >>> 33;
        mixed *= 0xff51afd7ed558ccdL;
        mixed ^= mixed >>> 33;
        mixed *= 0xc4ceb9fe1a85ec53L;
        mixed ^= mixed >>> 33;
        return mixed;
    }

    static final class Replica {
        private final int replicaId;
        private final AtomicBoolean enabled = new AtomicBoolean(true);
        private final ReplicaFileStore store;

        private Replica(int replicaId, Path replicaPath) {
            this.replicaId = replicaId;
            this.store = new ReplicaFileStore(replicaPath);
        }

        int id() {
            return replicaId;
        }

        boolean isEnabled() {
            return enabled.get();
        }

        void disable() {
            enabled.set(false);
        }

        void enable() {
            enabled.set(true);
        }

        VersionedEntry read(String key) throws IOException {
            return store.read(key);
        }

        boolean writeIfNewer(String key, VersionedEntry entry, boolean countAccess) throws IOException {
            return store.writeIfNewer(key, entry, countAccess);
        }

        ReplicaStats stats() {
            try {
                return store.stats(replicaId, isEnabled());
            } catch (IOException e) {
                throw new IllegalStateException("Failed to read replica stats", e);
            }
        }

        ReplicaAccessStats accessStats() {
            return store.accessStats(replicaId);
        }
    }
}
