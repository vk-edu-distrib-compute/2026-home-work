package company.vk.edu.distrib.compute.shuuuurik;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.shuuuurik.routing.RendezvousHashRouter;

import java.io.IOException;
import java.io.UncheckedIOException;
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
            try {
                KVService service = new KVServiceProxyImpl(port, new InMemoryDao(), endpoints, router);
                nodes.put(endpoint, service);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to create node on port " + port, e);
            }
        }

        return new ShuuuurikKVCluster(nodes, endpoints);
    }

    private List<String> portsToEndpoints(List<Integer> ports) {
        List<String> endpoints = new ArrayList<>(ports.size());
        for (int port : ports) {
            endpoints.add("http://localhost:" + port);
        }
        return endpoints;
    }
}
