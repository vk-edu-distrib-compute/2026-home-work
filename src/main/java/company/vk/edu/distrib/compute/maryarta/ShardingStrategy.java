package company.vk.edu.distrib.compute.maryarta;

public interface ShardingStrategy {
    public enum ShardingAlgorithm {
        CONSISTENT,
        RENDEZVOUS
    }
    String getEndpoint(String key);
}
