package company.vk.edu.distrib.compute.artttnik;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.artttnik.shard.RendezvousShardingStrategy;

import java.util.List;

public class MyKVClusterFactory extends KVClusterFactory {
    private final int replicaCount;

    public MyKVClusterFactory(int replicaCount) {
        super();
        this.replicaCount = replicaCount;
    }

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new MyKVCluster(ports, new RendezvousShardingStrategy(), replicaCount);
    }
}
