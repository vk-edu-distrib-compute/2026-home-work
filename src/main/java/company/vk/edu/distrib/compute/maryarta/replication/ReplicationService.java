package company.vk.edu.distrib.compute.maryarta.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

public class ReplicationService {
    private final int replicationFactor;
    private final Map<Integer, Boolean> replicas;
    private final Map<Integer, ReplicaNode> replicaNodes;
    private final AtomicLong versionGenerator = new AtomicLong();

    public ReplicationService(int replicationFactor, List<ReplicaNode> replicaNodes) {
        if (replicationFactor <= 0) {
            throw new IllegalArgumentException("Replication factor must be positive");
        }

        if (replicationFactor > replicaNodes.size()) {
            throw new IllegalArgumentException("Replication factor is greater than number of nodes");
        }

        this.replicationFactor = replicationFactor;
        this.replicas = new LinkedHashMap<>();
        this.replicaNodes = new LinkedHashMap<>();

        for (int i = 0; i < replicaNodes.size(); i++) {
            this.replicaNodes.put(i, replicaNodes.get(i));
            this.replicas.put(i, true);
        }
    }

    public boolean put(int ack, String key, byte[] value) throws IOException {
        validateAck(ack);
        validateKey(key);

        List<ReplicaNode> targetReplicas = availableReplicasForKey(key);

        long version = versionGenerator.incrementAndGet();
        int successfulWrites = 0;
        IOException lastException = null;

        for (ReplicaNode replica : targetReplicas) {
            replica.dao().upsert(key, value, version, false);
            successfulWrites++;
        }

        if (successfulWrites < ack) {
            if (lastException != null) {
                throw lastException;
            }

            throw new IllegalStateException("Not enough available replicas");
        }

        return true;
    }

    public byte[] get(int ack, String key) throws IOException {
        validateAck(ack);
        validateKey(key);

        List<ReplicaNode> targetReplicas = availableReplicasForKey(key);

        StoredRecord latest = null;
        int successfulReads = 0;
        IOException lastException = null;

        for (ReplicaNode replica : targetReplicas) {
            StoredRecord record = replica.dao().getRecord(key);
            successfulReads++;

            if (record != null && (latest == null || record.getVersion() > latest.getVersion())) {
                latest = record;
            }
        }

        if (successfulReads < ack) {
            if (lastException != null) {
                throw lastException;
            }

            throw new IllegalStateException("Not enough successful reads");
        }

        if (latest == null || latest.isDeleted()) {
            throw new NoSuchElementException("Key not found: " + key);
        }

        return latest.getData();
    }

    public boolean delete(int ack, String key) throws IOException {
        validateAck(ack);
        validateKey(key);

        List<ReplicaNode> targetReplicas = availableReplicasForKey(key);

        long version = versionGenerator.incrementAndGet();
        int successfulDeletes = 0;
        IOException lastException = null;

        for (ReplicaNode replica : targetReplicas) {
            replica.dao().delete(key, version);
            successfulDeletes++;
        }

        if (successfulDeletes < ack) {
            if (lastException != null) {
                throw lastException;
            }

            throw new IllegalStateException("Not enough successful deletes");
        }

        return true;
    }

    private List<ReplicaNode> availableReplicasForKey(String key) {
        List<Integer> replicaIds = replicasForKey(key);
        List<ReplicaNode> result = new ArrayList<>();

        for (Integer replicaId : replicaIds) {
            if (!replicas.getOrDefault(replicaId, false)) {
                continue;
            }

            ReplicaNode replicaNode = replicaNodes.get(replicaId);

            if (replicaNode != null) {
                result.add(replicaNode);
            }
        }

        return result;
    }

    private List<Integer> replicasForKey(String key) {
        if (replicationFactor > replicas.size()) {
            throw new IllegalStateException("Replication factor is greater than number of nodes");
        }

        int nodesCount = replicas.size();
        int start = Math.floorMod(key.hashCode(), nodesCount);

        List<Integer> result = new ArrayList<>(replicationFactor);

        for (int i = 0; i < replicationFactor; i++) {
            result.add((start + i) % nodesCount);
        }

        return result;
    }

    public void disableReplica(int nodeId) {
        if (!replicas.containsKey(nodeId)) {
            throw new IllegalArgumentException("Unknown replica: " + nodeId);
        }

        replicas.put(nodeId, false);
    }

    public void enableReplica(int nodeId) {
        if (!replicas.containsKey(nodeId)) {
            throw new IllegalArgumentException("Unknown replica: " + nodeId);
        }

        replicas.put(nodeId, true);
    }

    private void validateAck(int ack) {
        if (ack <= 0 || ack > replicationFactor) {
            throw new IllegalArgumentException("Invalid ack: " + ack);
        }
    }

    private static void validateKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key is blank");
        }
    }
}