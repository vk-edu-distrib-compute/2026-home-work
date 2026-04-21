package company.vk.edu.distrib.compute.maryarta;

@FunctionalInterface
public interface ShardingStrategy {
    enum ShardingAlgorithm {
        CONSISTENT,
        RENDEZVOUS
    }

    String getEndpoint(String key);
}
