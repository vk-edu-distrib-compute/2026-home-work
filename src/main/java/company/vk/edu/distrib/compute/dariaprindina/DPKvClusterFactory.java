package company.vk.edu.distrib.compute.dariaprindina;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

import java.util.List;

public class DPKvClusterFactory extends KVClusterFactory {
    private static final String ALGORITHM_PROPERTY = "dariaprindina.sharding.algorithm";
    private static final String REPLICATION_FACTOR_PROPERTY = "dariaprindina.replication.factor";
    private static final String REPLICATION_FACTOR_ENV = "DARIA_PRINDINA_REPLICATION_FACTOR";

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        final String configured = System.getProperty(ALGORITHM_PROPERTY);
        final DPShardingAlgorithm algorithm = DPShardingAlgorithm.fromProperty(configured);
        final int replicationFactor = resolveReplicationFactor();
        return new DPKvCluster(ports, algorithm, replicationFactor);
    }

    private static int resolveReplicationFactor() {
        final String propertyValue = System.getProperty(REPLICATION_FACTOR_PROPERTY);
        if (propertyValue != null && !propertyValue.isBlank()) {
            return Integer.parseInt(propertyValue);
        }
        final String envValue = System.getenv(REPLICATION_FACTOR_ENV);
        if (envValue != null && !envValue.isBlank()) {
            return Integer.parseInt(envValue);
        }
        return 1;
    }
}
