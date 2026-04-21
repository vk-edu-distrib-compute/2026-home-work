package company.vk.edu.distrib.compute.nihuaway00;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.nihuaway00.sharding.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NihuawayKVClusterFactory extends KVClusterFactory {
    private static final Logger log = LoggerFactory.getLogger(NihuawayKVClusterFactory.class);

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        Map<String, NodeInfo> nodes = new ConcurrentHashMap<>();
        ports.stream()
                .map(this::buildEndpoint)
                .forEach(endpoint -> nodes.put(endpoint, new NodeInfo(endpoint)));


        HttpClient httpClient = HttpClient.newHttpClient();

        String shardingStrategyProp = Config.strategy();
        if (log.isInfoEnabled()) {
            log.info("Using strategy: {}", shardingStrategyProp);
        }
        ShardingStrategy strategy = "rendezvous".equals(shardingStrategyProp)
                ? new RendezvousHashingStrategy(nodes)
                : new ConsistentHashingStrategy(nodes, 50);

        int replicaCountProps = Config.replicas();

        NihuawayKVServiceFactory serviceFactory = new NihuawayKVServiceFactory(strategy, httpClient, replicaCountProps);

        return new NihuawayKVCluster(strategy, serviceFactory);
    }

    private String buildEndpoint(int port) {
        return "http://localhost:" + port;
    }
}
