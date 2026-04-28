package company.vk.edu.distrib.compute.dariaprindina;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

import java.util.List;

public class DPKvClusterFactory extends KVClusterFactory {
    private static final String ALGORITHM_PROPERTY = "dariaprindina.sharding.algorithm";

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        final String configured = System.getProperty(ALGORITHM_PROPERTY);
        final DPShardingAlgorithm algorithm = DPShardingAlgorithm.fromProperty(configured);
        return new DPKvCluster(ports, algorithm);
    }
}
