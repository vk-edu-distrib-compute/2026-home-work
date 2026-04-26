package company.vk.edu.distrib.compute.maryarta.replication;

import company.vk.edu.distrib.compute.maryarta.H2Dao;
import company.vk.edu.distrib.compute.maryarta.sharding.ShardingStrategy;

import java.io.IOException;
import java.net.http.HttpClient;
import java.util.*;

public class ReplicationCoordinator {
    Map<String, Boolean> replicaNodes;
    ShardingStrategy shardingStrategy;
    int replicationFactor;
    ReplicaClient replicaClient;


    ReplicationCoordinator(List<String> endpoints, int replicationFactor, ShardingStrategy shardingStrategy, HttpClient httpClient, String selfEndpoint, H2Dao localDao){
        this.replicationFactor = replicationFactor;
        this.shardingStrategy = shardingStrategy;
        this.replicaClient = new ReplicaClient(httpClient, selfEndpoint, localDao);
    }

    public void upsert(int ack, String key, byte[] value) throws IOException {
        if (ack <= 0 || ack > replicationFactor) {
            throw new IllegalArgumentException("Invalid ack: " + ack);
        }
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key is blank");
        }
        List<String> replicas = shardingStrategy.getReplicaEndpoints(key, replicationFactor);
        long version = System.currentTimeMillis();
        int successfulWrites = 0;
        IOException lastException = null;
        for (String replica : replicas) {
            try {
                if (replicaClient.put(replica, key, value, version)) {
                    successfulWrites++;
                }
            } catch (IOException e) {
                lastException = e;
            }
        }
        if (successfulWrites < ack) {
            if (lastException != null) {
                throw lastException;
            }
            throw new IllegalStateException("Not enough replicas acknowledged write");
        }
    }

    public byte[] get(int ack, String key) throws IOException {
        if (ack <= 0 || ack > replicationFactor) {
            throw new IllegalArgumentException("Invalid ack: " + ack);
        }
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key is blank");
        }
        List<String> replicas = shardingStrategy.getReplicaEndpoints(key, replicationFactor);
        int successfulReads = 0;
        IOException lastException = null;
        StoredRecord latest = null;
        Map<String, StoredRecord> receivedRecords = new LinkedHashMap<>();
        for (String replica : replicas) {
            try {
                StoredRecord record = replicaClient.get(replica, key);
                successfulReads++;
                receivedRecords.put(replica, record);
                if (record != null && (latest == null || record.getVersion() > latest.getVersion())) {
                    latest = record;
                }
            } catch (IOException e) {
                lastException = e;
            }
        }
        if (successfulReads < ack) {
            if (lastException != null) {
                throw lastException;
            }
            throw new IllegalStateException("Not enough replicas acknowledged read");
        }
        repairStaleReplicas(key, latest, receivedRecords);
        if (latest == null || latest.isDeleted()) {
            throw new NoSuchElementException("Key not found: " + key);
        }
        return latest.getData();
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

            if (current == null || current.getVersion() < latest.getVersion()) {
                try {
                    if (latest.isDeleted()) {
                        replicaClient.delete(endpoint, key, latest.getVersion());
                    } else {
                        replicaClient.put(endpoint, key, latest.getData(), latest.getVersion());
                    }
                } catch (IOException e) {
                    // read repair best-effort: не ломаем успешный GET
                    // можно залогировать
                }
            }
        }
    }

    public void delete(int ack, String key) throws IOException {
        if (ack <= 0 || ack > replicationFactor) {
            throw new IllegalArgumentException("Invalid ack: " + ack);
        }
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key is blank");
        }
        List<String> replicas = shardingStrategy.getReplicaEndpoints(key, replicationFactor);
        long version = System.currentTimeMillis();
        int successfulDeletes = 0;
        IOException lastException = null;
        for (String replica : replicas) {
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




    void disableReplica(int nodeId) {

    }

    void enableReplica(int nodeId) {

    }

    public int numberOfReplicas() {
        return replicationFactor;
    }
}