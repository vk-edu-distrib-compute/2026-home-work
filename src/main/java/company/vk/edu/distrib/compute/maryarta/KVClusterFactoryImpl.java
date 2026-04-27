package company.vk.edu.distrib.compute.maryarta;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.maryarta.replication.ReplicatedKVClusterImpl;
import company.vk.edu.distrib.compute.maryarta.sharding.ShardingStrategy;

import java.util.List;

public class KVClusterFactoryImpl extends KVClusterFactory {
    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new KVClusterImpl(ports, ShardingStrategy.ShardingAlgorithm.CONSISTENT);
    }

    public KVCluster create(List<Integer> ports, int replicationFactor) {
        if (ports == null || ports.isEmpty()) {
            throw new IllegalArgumentException("Missing ports for the cluster");
        }

        return doCreate(ports, replicationFactor);
    }

    protected KVCluster doCreate(List<Integer> ports, int replicationFactor) {
        return new ReplicatedKVClusterImpl(ports, ShardingStrategy.ShardingAlgorithm.CONSISTENT, replicationFactor);
    }

}
