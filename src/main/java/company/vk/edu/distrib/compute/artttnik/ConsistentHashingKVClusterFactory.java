package company.vk.edu.distrib.compute.artttnik;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.artttnik.shard.ConsistentHashingStrategy;

import java.util.List;

public class ConsistentHashingKVClusterFactory extends KVClusterFactory {
    private final int replicaCount;

    public ConsistentHashingKVClusterFactory() {
        super();
        this.replicaCount = ReplicationConfig.resolveReplicaCount();
    }

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new MyKVCluster(ports, new ConsistentHashingStrategy(), replicaCount);
    }
}
