package company.vk.edu.distrib.compute.shuuuurik;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.shuuuurik.routing.RendezvousHashRouter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Создаёт кластер с Rendezvous Hashing (HRW).
 */
public class ShuuuurikRendezvousKVClusterFactory extends KVClusterFactory {

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        List<String> endpoints = portsToEndpoints(ports);
        RendezvousHashRouter router = new RendezvousHashRouter();

        Map<String, KVService> nodes = new ConcurrentHashMap<>();
        for (int port : ports) {
            String endpoint = "http://localhost:" + port;
            nodes.put(endpoint, createNode(port, endpoints, router));
        }

        return new ShuuuurikKVCluster(nodes, endpoints);
    }

    /**
     * Создаёт один узел кластера с собственным InMemoryDao.
     *
     * @param port      порт узла
     * @param endpoints все endpoint'ы кластера
     * @param router    алгоритм маршрутизации (общий для всех нод)
     * @return готовый KVService для данного узла
     */
    private KVService createNode(int port, List<String> endpoints, RendezvousHashRouter router) {
        return new KVServiceProxyImpl(port, new InMemoryDao(), endpoints, router);
    }

    private List<String> portsToEndpoints(List<Integer> ports) {
        List<String> endpoints = new ArrayList<>(ports.size());
        for (int port : ports) {
            endpoints.add("http://localhost:" + port);
        }
        return endpoints;
    }
}
