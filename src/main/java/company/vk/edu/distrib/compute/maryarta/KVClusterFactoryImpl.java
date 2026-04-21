package company.vk.edu.distrib.compute.maryarta;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

import java.util.List;

public class KVClusterFactoryImpl extends KVClusterFactory {
    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new KVClusterImpl(ports, ShardingStrategy.ShardingAlgorithm.CONSISTENT);
    }
}
