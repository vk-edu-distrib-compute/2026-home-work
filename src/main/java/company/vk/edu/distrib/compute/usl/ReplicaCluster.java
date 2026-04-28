package company.vk.edu.distrib.compute.usl;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

final class ReplicaCluster {
    private static final int MIN_REPLICA_COUNT = 3;
    private static final long FNV_OFFSET_BASIS = 0xcbf29ce484222325L;
    private static final long FNV_PRIME = 0x100000001b3L;

    private final List<Replica> replicas;

    ReplicaCluster(int replicaCount, Path storageRoot) throws IOException {
        if (replicaCount < MIN_REPLICA_COUNT) {
            throw new IllegalArgumentException(
                "Replication factor must be at least " + MIN_REPLICA_COUNT
            );
        }

        List<Replica> initializedReplicas = new ArrayList<>(replicaCount);
        for (int replicaId = 0; replicaId < replicaCount; replicaId++) {
            initializedReplicas.add(
                new Replica(replicaId, new ReplicaStore(storageRoot.resolve("replica-" + replicaId)))
            );
        }
        this.replicas = List.copyOf(initializedReplicas);
    }

    int replicaCount() {
        return replicas.size();
    }

    void validateAck(int ack) {
        if (ack <= 0 || ack > replicas.size()) {
            throw new IllegalArgumentException("Ack is out of range");
        }
    }

    Replica replica(int nodeId) {
        if (nodeId < 0 || nodeId >= replicas.size()) {
            throw new IllegalArgumentException("Replica id is out of range");
        }
        return replicas.get(nodeId);
    }

    List<Replica> orderedReplicasForKey(String key) {
        List<Replica> orderedReplicas = new ArrayList<>(replicas);
        orderedReplicas.sort((left, right) -> Long.compareUnsigned(
            rendezvousScore(key, right.id()),
            rendezvousScore(key, left.id())
        ));
        return orderedReplicas;
    }

    private static long rendezvousScore(String key, int replicaId) {
        long hash = FNV_OFFSET_BASIS;
        for (int index = 0; index < key.length(); index++) {
            hash ^= key.charAt(index);
            hash *= FNV_PRIME;
        }
        hash ^= '#';
        hash *= FNV_PRIME;
        hash ^= replicaId;
        hash *= FNV_PRIME;
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
        private final int id;
        private final ReplicaStore store;
        private final AtomicBoolean enabled = new AtomicBoolean(true);

        private Replica(int id, ReplicaStore store) {
            this.id = id;
            this.store = store;
        }

        int id() {
            return id;
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

        VersionedValue read(String key) throws IOException {
            return store.read(key);
        }

        boolean writeIfNewer(String key, VersionedValue value, boolean countAccess) throws IOException {
            return store.writeIfNewer(key, value, countAccess);
        }

        ReplicaStatsSnapshot stats() throws IOException {
            return store.stats(id, isEnabled());
        }

        ReplicaAccessStatsSnapshot accessStats() {
            return store.accessStats(id, isEnabled());
        }
    }
}
