package company.vk.edu.distrib.compute.maryarta.sharding;

import java.util.List;

@FunctionalInterface
public interface ShardingStrategy {
    enum ShardingAlgorithm {
        CONSISTENT,
        RENDEZVOUS
    }

    String getEndpoint(String key);

    default List<String> getReplicaEndpoints(String key, int replicationFactor) {
        return List.of();
    }
}
