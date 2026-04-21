package company.vk.edu.distrib.compute.shuuuurik.routing;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static company.vk.edu.distrib.compute.shuuuurik.util.HashUtils.hashPair;

/**
 * Детерминированно выбирает N реплик из общего пула узлов для данного ключа.
 * Использует Rendezvous Hashing: сортирует все узлы по убыванию hashPair(replicaId, key)
 * и берёт первые N.
 */
public class ReplicaRouter {

    private final int replicationFactor;

    /**
     * Создаёт роутер.
     *
     * @param replicationFactor фактор репликации N - сколько реплик хранят каждый ключ
     */
    public ReplicaRouter(int replicationFactor) {
        if (replicationFactor <= 0) {
            throw new IllegalArgumentException("replicationFactor must be > 0");
        }
        this.replicationFactor = replicationFactor;
    }

    /**
     * Возвращает до N индексов реплик в детерминированном порядке для данного ключа.
     * Индексы отсортированы по убыванию hashPair("replica-i", key).
     *
     * @param key           ключ запроса
     * @param totalReplicas общее количество узлов в кластере
     * @return список из min(N, totalReplicas) индексов реплик
     */
    public List<Integer> getReplicasForKey(String key, int totalReplicas) {
        int count = Math.min(replicationFactor, totalReplicas);

        List<Integer> allIndices = new ArrayList<>(totalReplicas);
        for (int i = 0; i < totalReplicas; i++) {
            allIndices.add(i);
        }

        allIndices.sort(Comparator.comparingLong(
                (Integer idx) -> hashPair("node-" + idx, key)).reversed());

        return new ArrayList<>(allIndices.subList(0, count));
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }
}
