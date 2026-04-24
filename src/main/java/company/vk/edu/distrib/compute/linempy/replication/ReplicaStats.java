package company.vk.edu.distrib.compute.linempy.replication;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Статистика обращений к репликам.
 *
 * @author Linempy
 * @since 24.04.2026
 */
public final class ReplicaStats {
    private final ConcurrentMap<Integer, ReplicaStatData> stats = new ConcurrentHashMap<>();

    public ReplicaStats(int replicationFactor) {
        for (int i = 0; i < replicationFactor; i++) {
            stats.put(i, new ReplicaStatData());
        }
    }

    public void recordRead(int replicaId) {
        stats.computeIfAbsent(replicaId, k -> new ReplicaStatData())
                .reads.incrementAndGet();
    }

    public void recordWrite(int replicaId) {
        stats.computeIfAbsent(replicaId, k -> new ReplicaStatData())
                .writes.incrementAndGet();
    }

    public long getReadCount(int replicaId) {
        ReplicaStatData data = stats.get(replicaId);
        return data == null ? 0 : data.reads.get();
    }

    public long getWriteCount(int replicaId) {
        ReplicaStatData data = stats.get(replicaId);
        return data == null ? 0 : data.writes.get();
    }

    private static final class ReplicaStatData {
        final AtomicLong reads = new AtomicLong(0);
        final AtomicLong writes = new AtomicLong(0);
    }
}
