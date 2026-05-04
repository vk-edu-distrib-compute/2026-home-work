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
    public static final String REPLICATION_FACTOR_PROPERTY = "che1nov.replication.factor";

    private final String shardingAlgorithm;
    private final int replicationFactor;

    public KVClusterFactoryImpl() {
        this(
                System.getProperty("che1nov.sharding.algorithm", ALGORITHM_RENDEZVOUS),
                Integer.parseInt(System.getProperty(REPLICATION_FACTOR_PROPERTY, "1"))
        );
    }

    public KVClusterFactoryImpl(String shardingAlgorithm) {
        this(shardingAlgorithm, Integer.parseInt(System.getProperty(REPLICATION_FACTOR_PROPERTY, "1")));
    }

    public KVClusterFactoryImpl(String shardingAlgorithm, int replicationFactor) {
        super();
        this.shardingAlgorithm = normalizeAlgorithm(shardingAlgorithm);
        this.replicationFactor = validateReplicationFactor(replicationFactor);
    }

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new KVClusterImpl(ports, shardingAlgorithm, replicationFactor);
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

    private static int validateReplicationFactor(int replicationFactor) {
        if (replicationFactor <= 0) {
            throw new IllegalArgumentException("replicationFactor must be positive");
        }
        return replicationFactor;
    }
}
