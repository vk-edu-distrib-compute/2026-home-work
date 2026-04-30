package company.vk.edu.distrib.compute.nihuaway00.bootstrap;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.nihuaway00.Config;
import company.vk.edu.distrib.compute.nihuaway00.Cluster;
import company.vk.edu.distrib.compute.nihuaway00.cluster.*;
import company.vk.edu.distrib.compute.nihuaway00.transport.grpc.GrpcChannelRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterFactory extends KVClusterFactory {
    private static final Logger log = LoggerFactory.getLogger(ClusterFactory.class);

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        Map<String, ClusterNode> nodes = new ConcurrentHashMap<>();

        ports.forEach(port -> {
            String endpoint = "http://localhost:" + (port);
            String grpcEndpoint = "localhost:" + (port + 1);
            nodes.put(endpoint, new ClusterNode(endpoint, grpcEndpoint));
        });

        GrpcChannelRegistry channelRegistry = new GrpcChannelRegistry();

        String shardingStrategyProp = Config.strategy();
        if (log.isInfoEnabled()) {
            log.info("Using strategy: {}", shardingStrategyProp);
        }
        ShardingStrategy strategy = "rendezvous".equals(shardingStrategyProp)
                ? new RendezvousHashingStrategy(nodes)
                : new ConsistentHashingStrategy(nodes, 50);

        int replicaCountProps = Config.replicas();

        ServiceFactory serviceFactory = new ServiceFactory(strategy, replicaCountProps, channelRegistry);

        return new Cluster(strategy, serviceFactory, channelRegistry);
    }
}
