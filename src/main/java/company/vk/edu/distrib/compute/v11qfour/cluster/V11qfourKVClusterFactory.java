package company.vk.edu.distrib.compute.v11qfour.cluster;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.v11qfour.dao.V11qfourPersistentDao;
import company.vk.edu.distrib.compute.v11qfour.proxy.V11qfourProxyClient;
import company.vk.edu.distrib.compute.v11qfour.service.V11qfourKVServiceFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class V11qfourKVClusterFactory {
    public V11qfourKVCluster create(List<Integer> ports) throws IOException {
        String algo = System.getProperty("algo", "rendezvous");
        List<V11qfourNode> allNodes = ports.stream()
                .map(p -> new V11qfourNode("http://localhost:" + p))
                .toList();
        V11qfourRoutingStrategy strategy;
        if ("consistent".equals(algo)) {
            strategy = new ConsistentHashing(allNodes);
        } else {
            strategy = new RendezvousHashing();
        }
        Map<String, KVService> nodesMap = ports.stream()
                .collect(Collectors.toConcurrentMap(
                        port -> "http://localhost:" + port,
                        port -> {
                            try {
                                return new V11qfourKVServiceFactory(
                                        port,
                                        new V11qfourPersistentDao(),
                                        strategy,
                                        allNodes,
                                        "http://localhost:" + port,
                                        new V11qfourProxyClient()
                                );
                            } catch (IOException e) {
                                throw new IllegalStateException(e);
                            }
                        }
                ));
        return new V11qfourKVCluster(nodesMap);
    }
}
