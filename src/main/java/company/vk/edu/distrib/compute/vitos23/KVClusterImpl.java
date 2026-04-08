package company.vk.edu.distrib.compute.vitos23;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.vitos23.util.HttpUtils;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KVClusterImpl implements KVCluster {

    private final KVServiceFactory kvServiceFactory;
    private final Map<String, Integer> portByEndpoint;
    /// Dynamic dictionary of started nodes.
    /// The value is created when the node starts and is deleted when it stops.
    /// Unfortunately, this was not a very pretty solution that had to be implemented,
    /// because in tests the node can be started after stopping, which [KVService] prohibits.
    private final Map<String, KVService> kvServiceByEndpoint;

    public KVClusterImpl(KVServiceFactory kvServiceFactory, List<Integer> ports) {
        this.kvServiceFactory = kvServiceFactory;
        this.portByEndpoint = ports.stream()
                .collect(Collectors.toMap(HttpUtils::getLocalEndpoint, Function.identity()));
        this.kvServiceByEndpoint = LinkedHashMap.newLinkedHashMap(ports.size());
    }

    @Override
    public void start() {
        portByEndpoint.keySet().forEach(this::start);
    }

    @Override
    public void start(String endpoint) {
        Integer port = portByEndpoint.get(endpoint);
        if (port == null) {
            throw new IllegalArgumentException("Unknown node");
        }
        if (kvServiceByEndpoint.containsKey(endpoint)) {
            return;
        }
        try {
            KVService node = kvServiceFactory.create(port);
            node.start();
            kvServiceByEndpoint.put(endpoint, node);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        portByEndpoint.keySet().forEach(this::stop);
    }

    @Override
    public void stop(String endpoint) {
        if (!portByEndpoint.containsKey(endpoint)) {
            throw new IllegalArgumentException("Unknown node");
        }
        KVService node = kvServiceByEndpoint.get(endpoint);
        if (node == null) {
            return;
        }
        node.stop();
        kvServiceByEndpoint.remove(endpoint);
    }

    @Override
    public List<String> getEndpoints() {
        return List.copyOf(portByEndpoint.keySet());
    }
}
