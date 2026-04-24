package company.vk.edu.distrib.compute.linempy.replication;

import java.util.ArrayList;
import java.util.List;

/**
 * Детерминированный выбор реплик по хешу ключа.
 *
 * @author Linempy
 * @since 24.04.2026
 */
public class ReplicaSelector {
    private final int replicationFactor;
    private final int maxReplicas;

    public ReplicaSelector(int replicationFactor) {
        this.replicationFactor = replicationFactor;
        this.maxReplicas = 1000;
    }

    private int hash(String key) {
        return Math.abs(key.hashCode()) % maxReplicas;
    }

    public List<Integer> getReplicaIndexes(String key) {
        int start = hash(key);
        List<Integer> indexes = new ArrayList<>(replicationFactor);

        for (int i = 0; i < replicationFactor; i++) {
            int index = (start + i) % maxReplicas;
            indexes.add(index);
        }
        return indexes;
    }
}
