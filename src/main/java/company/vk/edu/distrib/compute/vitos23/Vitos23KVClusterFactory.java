package company.vk.edu.distrib.compute.vitos23;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.vitos23.shard.ConsistentHashingShardResolver;
import company.vk.edu.distrib.compute.vitos23.shard.ShardResolver;
import company.vk.edu.distrib.compute.vitos23.shard.ShardedKVServiceFactory;
import company.vk.edu.distrib.compute.vitos23.util.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Vitos23KVClusterFactory extends KVClusterFactory {

    private static final Logger log = LoggerFactory.getLogger(Vitos23KVClusterFactory.class);
    private static final int DEFAULT_REPLICATION_FACTOR = 1;

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        int replicationFactor = getReplicationFactor();
        if (replicationFactor > ports.size()) {
            throw new IllegalArgumentException("Replication factor cannot exceed the number of shards");
        }
        List<String> shards = ports.stream().map(HttpUtils::getLocalEndpoint).toList();
        // ATTENTION: sharding algorithm choice
        // ShardResolver shardResolver = new RendezvousShardResolver(shards);
        ShardResolver shardResolver = new ConsistentHashingShardResolver(shards);
        ShardedKVServiceFactory shardedKVServiceFactory = new ShardedKVServiceFactory(shardResolver, replicationFactor);
        return new KVClusterImpl(shardedKVServiceFactory, ports);
    }

    private int getReplicationFactor() {
        String replicationFactor = System.getenv("REPLICATION_FACTOR");
        if (replicationFactor == null) {
            return DEFAULT_REPLICATION_FACTOR;
        }
        try {
            return Integer.parseInt(replicationFactor);
        } catch (NumberFormatException e) {
            log.warn("Failed to parse replication factor '{}', switching to default", replicationFactor);
            return DEFAULT_REPLICATION_FACTOR;
        }
    }
}
