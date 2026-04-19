package company.vk.edu.distrib.compute.gavrilova_ekaterina.replication;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

public class ReplicaSynchronizer {

    private final List<ReplicaNode> replicas;

    public ReplicaSynchronizer(List<ReplicaNode> replicas) {
        this.replicas = replicas;
    }

    public void syncReplica(ReplicaNode target, Set<String> keys) throws IOException {
        for (String key : keys) {
            syncKey(target, key);
        }
    }

    private void syncKey(ReplicaNode target, String key) throws IOException {
        byte[] value = null;
        for (ReplicaNode node : replicas) {
            if (!node.alive || node == target) {
                continue;
            }
            try {
                value = node.dao.get(key);
                if (value != null) {
                    break;
                }
            } catch (NoSuchElementException e) {
                target.dao.delete(key);
            }
        }
        if (value != null) {
            target.dao.upsert(key, value);
        } else {
            target.dao.delete(key);
        }
    }

}
