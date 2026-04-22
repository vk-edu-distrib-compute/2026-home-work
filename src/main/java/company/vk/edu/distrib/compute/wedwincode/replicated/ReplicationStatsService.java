package company.vk.edu.distrib.compute.wedwincode.replicated;

import java.util.concurrent.atomic.AtomicIntegerArray;

public class ReplicationStatsService {
    private final AtomicIntegerArray keysByReplica;
    private final AtomicIntegerArray requestsByReplica;
    private final long startTime;

    public ReplicationStatsService(int replicasCount) {
        keysByReplica = new AtomicIntegerArray(replicasCount);
        requestsByReplica = new AtomicIntegerArray(replicasCount);
        startTime = System.currentTimeMillis();
    }

    public void incrementKeysCount(int replicaId) {
        keysByReplica.addAndGet(replicaId, 1);
    }

    public int getKeysCount(int replicaId) {
        return keysByReplica.get(replicaId);
    }

    public void incrementRequestCount(int replicaId) {
        requestsByReplica.addAndGet(replicaId, 1);
    }

    public float getAccessRate(int replicaId) {
        long timeDelta = (System.currentTimeMillis() - startTime) * 1000;
        return (float) requestsByReplica.get(replicaId) / timeDelta;
    }
}
