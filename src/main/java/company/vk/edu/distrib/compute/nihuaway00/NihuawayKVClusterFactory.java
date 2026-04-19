package company.vk.edu.distrib.compute.nihuaway00;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.dummy.DummyKVService;
import company.vk.edu.distrib.compute.nihuaway00.sharding.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.http.HttpClient;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class NihuawayKVClusterFactory extends KVClusterFactory {
    private static final Logger log = LoggerFactory.getLogger(DummyKVService.class);

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        Map<String, NodeInfo> nodes = new ConcurrentHashMap<>();
        ports.stream()
                .map(this::buildEndpoint)
                .forEach(endpoint -> nodes.put(endpoint, new NodeInfo(endpoint)));

        String val = loadStrategy();
        if (log.isInfoEnabled()) {
            log.info("Using strategy: {}", val);
        }

        HttpClient httpClient = HttpClient.newHttpClient();

        ShardingStrategy strategy = "rendezvous".equals(val)
                ? new RendezvousHashingStrategy(nodes)
                : new ConsistentHashingStrategy(nodes, 50);

        NihuawayKVServiceFactory serviceFactory = new NihuawayKVServiceFactory(strategy, httpClient);

        return new NihuawayKVCluster(strategy, serviceFactory);
    }

    private String loadStrategy() {
        try (var is = getClass().getClassLoader().getResourceAsStream("nihuaway00.properties")) {
            if (is != null) {
                Properties props = new Properties();
                props.load(is);
                return props.getProperty("strategy", "consistent");
            }
        } catch (IOException e) {
            if (log.isErrorEnabled()) {
                log.error("NihuawayKVClusterFactory loadStrategy error", e);
            }
        }

        return "consistent";
    }

    private String buildEndpoint(int port) {
        return "http://localhost:" + port;
    }
}
