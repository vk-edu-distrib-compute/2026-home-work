package company.vk.edu.distrib.compute.tadzhnahal;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

import java.util.List;

public class TadzhnahalKVClusterFactory extends KVClusterFactory {
    private final TadzhnahalShardingAlgorithm shardingAlgorithm;

    public TadzhnahalKVClusterFactory() {
        super();
        this.shardingAlgorithm = TadzhnahalShardingAlgorithm.RENDEZVOUS;
    }

    public TadzhnahalKVClusterFactory(TadzhnahalShardingAlgorithm shardingAlgorithm) {
        super();

        if (shardingAlgorithm == null) {
            throw new IllegalArgumentException("Sharding algorithm must not be null");
        }

        this.shardingAlgorithm = shardingAlgorithm;
    }

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new TadzhnahalKVCluster(ports, shardingAlgorithm);
    }
}
