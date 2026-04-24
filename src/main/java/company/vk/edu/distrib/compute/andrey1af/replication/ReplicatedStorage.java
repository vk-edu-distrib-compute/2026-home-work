package company.vk.edu.distrib.compute.andrey1af.replication;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

final class ReplicatedStorage {
    private static final int MIN_REPLICA_COUNT = 1;
    private static final int FIRST_REPLICA_INDEX = 0;
    private static final boolean REPLICA_ENABLED = true;

    private final Replica[] replicas;
    private final AtomicLong versionSequence = new AtomicLong();

    ReplicatedStorage(int replicaCount) {
        if (replicaCount < MIN_REPLICA_COUNT) {
            throw new IllegalArgumentException("replicaCount must be positive");
        }

        this.replicas = new Replica[replicaCount];
        for (int i = 0; i < replicaCount; i++) {
            replicas[i] = new Replica();
        }
    }

    int numberOfReplicas() {
        return replicas.length;
    }

    void disableReplica(int nodeId) {
        replica(nodeId).disable();
    }

    void enableReplica(int nodeId) {
        replica(nodeId).enable();
    }

    int upsert(String id, byte[] value) {
        VersionedRecord record = VersionedRecord.value(value, versionSequence.incrementAndGet());
        return write(id, record);
    }

    int delete(String id) {
        VersionedRecord record = VersionedRecord.tombstone(versionSequence.incrementAndGet());
        return write(id, record);
    }

    ReadResult get(String id) {
        int responses = 0;
        VersionedRecord freshest = null;

        for (Replica replica : replicasForKey(id)) {
            if (!replica.isEnabled()) {
                continue;
            }

            responses++;
            VersionedRecord record = replica.get(id);
            if (record != null && (freshest == null || record.version() > freshest.version())) {
                freshest = record;
            }
        }

        return new ReadResult(responses, freshest);
    }

    private int write(String id, VersionedRecord record) {
        int successfulWrites = 0;
        for (Replica replica : replicasForKey(id)) {
            if (!replica.isEnabled()) {
                continue;
            }

            replica.put(id, record);
            successfulWrites++;
        }
        return successfulWrites;
    }

    private Replica[] replicasForKey(String id) {
        Objects.requireNonNull(id, "id cannot be null");
        return replicas;
    }

    private Replica replica(int nodeId) {
        if (nodeId < FIRST_REPLICA_INDEX || nodeId >= replicas.length) {
            throw new IllegalArgumentException("Unknown replica: " + nodeId);
        }
        return replicas[nodeId];
    }

    record ReadResult(int responses, VersionedRecord record) {
    }

    record VersionedRecord(byte[] value, long version, boolean tombstone) {
        VersionedRecord {
            if (!tombstone) {
                value = Arrays.copyOf(value, value.length);
            }
        }

        static VersionedRecord value(byte[] value, long version) {
            return new VersionedRecord(value, version, false);
        }

        static VersionedRecord tombstone(long version) {
            return new VersionedRecord(null, version, true);
        }

        @Override
        public byte[] value() {
            if (value == null) {
                return null;
            }
            return Arrays.copyOf(value, value.length);
        }
    }

    private static final class Replica {
        private final Map<String, VersionedRecord> storage = new ConcurrentHashMap<>();
        private final AtomicBoolean enabled = new AtomicBoolean(REPLICA_ENABLED);

        boolean isEnabled() {
            return enabled.get();
        }

        void enable() {
            enabled.set(true);
        }

        void disable() {
            enabled.set(false);
        }

        VersionedRecord get(String id) {
            return storage.get(id);
        }

        void put(String id, VersionedRecord record) {
            storage.put(id, record);
        }
    }
}
