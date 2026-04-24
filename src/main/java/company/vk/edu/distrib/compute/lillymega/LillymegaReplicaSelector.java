package company.vk.edu.distrib.compute.lillymega;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

final class LillymegaReplicaSelector {
    private final int replicationFactor;

    LillymegaReplicaSelector(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    List<Integer> selectReplicas(String key) {
        List<Integer> selectedReplicas = new ArrayList<>(replicationFactor);
        Set<Integer> usedReplicaIds = new HashSet<>();
        long nonce = 0;

        while (selectedReplicas.size() < replicationFactor) {
            int replicaId = Math.floorMod(Long.hashCode(hash(key, nonce)), replicationFactor);
            if (usedReplicaIds.add(replicaId)) {
                selectedReplicas.add(replicaId);
            }
            nonce++;
        }

        return selectedReplicas;
    }

    private long hash(String key, long nonce) {
        String value = key + "#" + nonce;
        long hash = 0xcbf29ce484222325L;
        for (int i = 0; i < value.length(); i++) {
            hash ^= value.charAt(i);
            hash *= 0x100000001b3L;
        }
        return hash;
    }
}
