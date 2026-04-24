package company.vk.edu.distrib.compute.ce_fello;

import java.util.ArrayList;
import java.util.List;

final class CeFelloReplicaPicker {
    private final int totalReplicas;

    CeFelloReplicaPicker(int totalReplicas) {
        if (totalReplicas <= 0) {
            throw new IllegalArgumentException("totalReplicas must be positive");
        }
        this.totalReplicas = totalReplicas;
    }

    List<Integer> pick(String key, int replicationFactor) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key must be non-empty");
        }
        if (replicationFactor <= 0 || replicationFactor > totalReplicas) {
            throw new IllegalArgumentException("Invalid replication factor");
        }

        int start = (int) Long.remainderUnsigned(CeFelloHashing.replicaHash64(key), totalReplicas);
        List<Integer> result = new ArrayList<>(replicationFactor);
        for (int i = 0; i < replicationFactor; i++) {
            result.add((start + i) % totalReplicas);
        }
        return List.copyOf(result);
    }
}
