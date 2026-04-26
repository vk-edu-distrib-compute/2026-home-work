package company.vk.edu.distrib.compute.maryarta;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.maryarta.replication.ReplicatedKVClusterImpl;
import company.vk.edu.distrib.compute.maryarta.sharding.ShardingStrategy;

import java.util.List;

public class KVClusterFactoryImpl extends KVClusterFactory {
    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new ReplicatedKVClusterImpl(ports, ShardingStrategy.ShardingAlgorithm.CONSISTENT, 3);
    }
}
