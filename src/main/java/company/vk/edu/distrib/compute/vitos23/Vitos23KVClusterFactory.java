package company.vk.edu.distrib.compute.vitos23;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.vitos23.shard.ConsistentHashingShardResolver;
import company.vk.edu.distrib.compute.vitos23.shard.ShardInfo;
import company.vk.edu.distrib.compute.vitos23.shard.ShardResolver;
import company.vk.edu.distrib.compute.vitos23.shard.ShardedKVServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

import static company.vk.edu.distrib.compute.vitos23.util.HttpUtils.findFreePorts;
import static company.vk.edu.distrib.compute.vitos23.util.HttpUtils.getLocalEndpoint;

public class Vitos23KVClusterFactory extends KVClusterFactory {

    private static final Logger log = LoggerFactory.getLogger(Vitos23KVClusterFactory.class);

    private static final int DEFAULT_REPLICATION_FACTOR = 1;
    private static final Duration AFTER_PORT_FREE_DELAY = Duration.ofMillis(10);

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        int replicationFactor = getReplicationFactor();
        if (replicationFactor > ports.size()) {
            throw new IllegalArgumentException("Replication factor cannot exceed the number of shards");
        }
        List<NodePorts> nodesPorts = getNodePorts(ports);
        List<ShardInfo> shards = getShardInfos(nodesPorts);
        // ATTENTION: sharding algorithm choice
        // ShardResolver shardResolver = new RendezvousShardResolver(shards);
        ShardResolver shardResolver = new ConsistentHashingShardResolver(shards);
        var shardedKVServiceFactory = new ShardedKVServiceFactory(shardResolver, replicationFactor);
        var clusterKVServiceFactory = new ClusterKVServiceFactory(shardedKVServiceFactory);
        return new KVClusterImpl(clusterKVServiceFactory, nodesPorts);
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

    private List<NodePorts> getNodePorts(List<Integer> httpPorts) {
        try {
            Queue<Integer> freePorts = new ArrayDeque<>(findFreePorts(httpPorts.size()));
            try {
                Thread.sleep(AFTER_PORT_FREE_DELAY);
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
            return httpPorts.stream()
                    .map(httpPort -> new NodePorts(httpPort, freePorts.remove()))
                    .toList();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to find free ports", e);
        }
    }

    private List<ShardInfo> getShardInfos(List<NodePorts> nodesPorts) {
        return nodesPorts.stream()
                .map(ports -> new ShardInfo(
                        getLocalEndpoint(ports.httpPort()),
                        "localhost:" + ports.grpcPort()
                ))
                .toList();
    }

}
