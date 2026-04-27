package company.vk.edu.distrib.compute.maryarta.replication;

import company.vk.edu.distrib.compute.maryarta.H2Dao;
import company.vk.edu.distrib.compute.maryarta.sharding.ShardingStrategy;

import java.io.IOException;
import java.net.http.HttpClient;
import java.util.*;

public class ReplicationCoordinator {
    private final ShardingStrategy shardingStrategy;
    private final int replicationFactor;
    private final ReplicaClient replicaClient;
    private final List<String> endpoints;
    private final boolean[] disabledReplicas;

    public ReplicationCoordinator(List<String> endpoints,
                                   int replicationFactor,
                                   ShardingStrategy shardingStrategy,
                                   HttpClient httpClient,
                                   String selfEndpoint,
                                   H2Dao localDao) {
        this.replicationFactor = replicationFactor;
        this.shardingStrategy = shardingStrategy;
        this.replicaClient = new ReplicaClient(httpClient, selfEndpoint, localDao);
        this.endpoints = endpoints;
        this.disabledReplicas = new boolean[endpoints.size()];
        Arrays.fill(this.disabledReplicas, false);
    }

    public void upsert(int ack, String key, byte[] value) throws IOException {
        if (ack <= 0 || ack > replicationFactor) {
            throw new IllegalArgumentException("Invalid ack: " + ack);
        }
        List<String> replicas = shardingStrategy.getReplicaEndpoints(key, replicationFactor);
        long version = System.currentTimeMillis();
        int successfulWrites = 0;
        for (String replica : replicas) {
            if (isReplicaDisabled(replica)) {
                continue;
            }
            if (replicaClient.put(replica, key, value, version)) {
                successfulWrites++;
            }
        }
        if (successfulWrites < ack) {
            throw new IllegalStateException("Not enough replicas acknowledged write");
        }
    }

    @SuppressWarnings("PMD.UseConcurrentHashMap") // Local map, no concurrent access
    public byte[] get(int ack, String key) throws IOException {
        if (ack <= 0 || ack > replicationFactor) {
            throw new IllegalArgumentException("Invalid ack: " + ack);
        }
        List<String> replicas = shardingStrategy.getReplicaEndpoints(key, replicationFactor);
        int successfulReads = 0;
        StoredRecord latest = null;
        Map<String, StoredRecord> receivedRecords = new HashMap<>();
        for (String replica : replicas) {
            if (isReplicaDisabled(replica)) {
                continue;
            }
                StoredRecord record = replicaClient.get(replica, key);
                successfulReads++;
                receivedRecords.put(replica, record);
                if (record != null && (latest == null || record.version() > latest.version())) {
                    latest = record;
                }
        }
        if (successfulReads < ack) {
            throw new IllegalStateException("Not enough replicas acknowledged read");
        }
        repairStaleReplicas(key, latest, receivedRecords);
        if (latest == null || latest.deleted()) {
            throw new NoSuchElementException("Key not found: " + key);
        }
        return latest.data();
    }

    private void repairStaleReplicas(
            String key,
            StoredRecord latest,
            Map<String, StoredRecord> receivedRecords
    ) {
        if (latest == null) {
            return;
        }
        for (Map.Entry<String, StoredRecord> entry : receivedRecords.entrySet()) {
            String endpoint = entry.getKey();
            StoredRecord current = entry.getValue();

            if (current == null || current.version() < latest.version()) {
                try {
                    if (latest.deleted()) {
                        replicaClient.delete(endpoint, key, latest.version());
                    } else {
                        replicaClient.put(endpoint, key, latest.data(), latest.version());
                    }
                } catch (IOException ignored) {
                    // failed repair
                }
            }
        }
    }

    public void delete(int ack, String key) throws IOException {
        if (ack <= 0 || ack > replicationFactor) {
            throw new IllegalArgumentException("Invalid ack: " + ack);
        }
        List<String> replicas = shardingStrategy.getReplicaEndpoints(key, replicationFactor);
        long version = System.currentTimeMillis();
        int successfulDeletes = 0;
        IOException lastException = null;
        for (String replica : replicas) {
            if (isReplicaDisabled(replica)) {
                continue;
            }
            try {
                if (replicaClient.delete(replica, key, version)) {
                    successfulDeletes++;
                }
            } catch (IOException e) {
                lastException = e;
            }
        }
        if (successfulDeletes < ack) {
            if (lastException != null) {
                throw lastException;
            }
            throw new IllegalStateException("Not enough replicas acknowledged delete");
        }
    }

    private boolean isReplicaDisabled(String endpoint) {
        int nodeId = endpoints.indexOf(endpoint);
        if (nodeId < 0) {
            throw new IllegalStateException("Unknown endpoint: " + endpoint);
        }
        return disabledReplicas[nodeId];
    }

    void disableReplica(int nodeId) {
        disabledReplicas[nodeId] = true;
    }

    void enableReplica(int nodeId) {
        disabledReplicas[nodeId] = false;
    }

    public int numberOfReplicas() {
        return replicationFactor;
    }
}
