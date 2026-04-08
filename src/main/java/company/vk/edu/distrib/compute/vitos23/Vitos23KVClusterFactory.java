package company.vk.edu.distrib.compute.vitos23;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.vitos23.shard.RendezvousShardResolver;
import company.vk.edu.distrib.compute.vitos23.shard.ShardResolver;
import company.vk.edu.distrib.compute.vitos23.shard.ShardedKVServiceFactory;
import company.vk.edu.distrib.compute.vitos23.util.HttpUtils;

import java.util.List;

public class Vitos23KVClusterFactory extends KVClusterFactory {
    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        List<String> shards = ports.stream().map(HttpUtils::getLocalEndpoint).toList();
        // ATTENTION: sharding algorithm choice
        ShardResolver shardResolver = new RendezvousShardResolver(shards);
        // ShardResolver shardResolver = new ConsistentHashingShardResolver(shards);
        ShardedKVServiceFactory shardedKVServiceFactory = new ShardedKVServiceFactory(shardResolver);
        return new KVClusterImpl(shardedKVServiceFactory, ports);
    }
}
