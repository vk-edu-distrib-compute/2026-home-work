package company.vk.edu.distrib.compute.v11qfour.cluster;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.v11qfour.dao.V11qfourPersistentDao;
import company.vk.edu.distrib.compute.v11qfour.proxy.V11qfourProxyClient;
import company.vk.edu.distrib.compute.v11qfour.service.V11qfourKVServiceFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class V11qfourKVClusterFactory {
    public V11qfourKVCluster create(List<Integer> ports) throws IOException {
        Map<String, KVService> nodesMap = new ConcurrentHashMap<>();
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
        for (int port : ports) {
            String selfUrl = "http://localhost:" + port;
            Dao<byte[]> dao = new V11qfourPersistentDao();
            V11qfourKVServiceFactory service = new V11qfourKVServiceFactory(
                    port,
                    dao,
                    strategy,
                    allNodes,
                    selfUrl,
                    new V11qfourProxyClient()
            );
            nodesMap.put(selfUrl, service);
        }

        return new V11qfourKVCluster(nodesMap);
    }
}
