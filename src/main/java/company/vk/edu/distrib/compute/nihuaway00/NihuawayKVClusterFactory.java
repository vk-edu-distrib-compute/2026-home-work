package company.vk.edu.distrib.compute.nihuaway00;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.nihuaway00.sharding.ConsistentHashingStrategy;
import company.vk.edu.distrib.compute.nihuaway00.sharding.NodeInfo;
import company.vk.edu.distrib.compute.nihuaway00.sharding.RendezvousHashingStrategy;
import company.vk.edu.distrib.compute.nihuaway00.sharding.ShardingStrategy;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

public class NihuawayKVClusterFactory extends KVClusterFactory {
    public NihuawayKVClusterFactory() {
    }

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        var log = LoggerFactory.getLogger("server");
        CopyOnWriteArrayList<NodeInfo> nodes = new CopyOnWriteArrayList<>(ports.stream()
                .map(p -> new NodeInfo("http://localhost:" + p, p))
                .toList());

        String val = loadStrategy();
        log.info("Using strategy: {}", val);

        ShardingStrategy strategy = switch (val) {
            case "rendezvous" -> new RendezvousHashingStrategy(nodes);
            default -> new ConsistentHashingStrategy(nodes, 5);
        };


        NihuawayKVServiceFactory serviceFactory = new NihuawayKVServiceFactory(strategy);
        return new NihuawayKVCluster(nodes, serviceFactory);
    }

    private String loadStrategy() {
        try (var is = getClass().getClassLoader().getResourceAsStream("nihuaway00.properties")) {
            if (is != null) {
                Properties props = new Properties();
                props.load(is);
                return props.getProperty("strategy", "consistent");
            }
        } catch (Exception _) {
        }

        return "consistent";
    }
}
