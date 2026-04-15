package company.vk.edu.distrib.compute.che1nov;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.che1nov.cluster.ConsistentHashRouter;
import company.vk.edu.distrib.compute.che1nov.cluster.RendezvousHashRouter;
import company.vk.edu.distrib.compute.che1nov.cluster.ShardRouter;

import java.util.List;
import java.util.Locale;

public class KVClusterFactoryImpl extends KVClusterFactory {
    public static final String ALGORITHM_RENDEZVOUS = "rendezvous";
    public static final String ALGORITHM_CONSISTENT = "consistent";

    private final String shardingAlgorithm;

    public KVClusterFactoryImpl() {
        this(System.getProperty("che1nov.sharding.algorithm", ALGORITHM_RENDEZVOUS));
    }

    public KVClusterFactoryImpl(String shardingAlgorithm) {
        super();
        this.shardingAlgorithm = normalizeAlgorithm(shardingAlgorithm);
    }

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new KVClusterImpl(ports, shardingAlgorithm);
    }

    static ShardRouter createRouter(List<String> endpoints, String algorithm) {
        return switch (normalizeAlgorithm(algorithm)) {
            case ALGORITHM_CONSISTENT -> new ConsistentHashRouter(endpoints);
            case ALGORITHM_RENDEZVOUS -> new RendezvousHashRouter(endpoints);
            default -> throw new IllegalArgumentException("Unknown sharding algorithm: " + algorithm);
        };
    }

    private static String normalizeAlgorithm(String algorithm) {
        if (algorithm == null || algorithm.isBlank()) {
            throw new IllegalArgumentException("sharding algorithm must not be null or blank");
        }
        return algorithm.toLowerCase(Locale.ROOT);
    }
}
