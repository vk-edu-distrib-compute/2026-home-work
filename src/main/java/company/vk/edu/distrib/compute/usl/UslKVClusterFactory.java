package company.vk.edu.distrib.compute.usl;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.usl.sharding.ShardingAlgorithm;

import java.util.List;
import java.util.Objects;

public class UslKVClusterFactory extends KVClusterFactory {
    public static final String SHARDING_ALGORITHM_PROPERTY =
        "company.vk.edu.distrib.compute.usl.sharding.algorithm";
    public static final String SHARDING_ALGORITHM_ENV =
        "COMPANY_VK_EDU_DISTRIB_COMPUTE_USL_SHARDING_ALGORITHM";

    private final ShardingAlgorithm algorithm;

    public UslKVClusterFactory() {
        super();
        this.algorithm = resolveConfiguredAlgorithm();
    }

    public UslKVClusterFactory(ShardingAlgorithm algorithm) {
        super();
        this.algorithm = Objects.requireNonNull(algorithm);
    }

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new UslKVCluster(
            ports,
            algorithm.createStrategy(ports.stream().map(UslNodeServer::endpointUrl).toList())
        );
    }

    private static ShardingAlgorithm resolveConfiguredAlgorithm() {
        String propertyValue = System.getProperty(SHARDING_ALGORITHM_PROPERTY);
        if (propertyValue != null && !propertyValue.isBlank()) {
            return ShardingAlgorithm.fromExternalName(propertyValue);
        }

        String envValue = System.getenv(SHARDING_ALGORITHM_ENV);
        return ShardingAlgorithm.fromExternalName(envValue);
    }
}
