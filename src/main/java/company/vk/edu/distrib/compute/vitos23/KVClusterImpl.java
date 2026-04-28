package company.vk.edu.distrib.compute.vitos23;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.vitos23.exception.ServerException;
import company.vk.edu.distrib.compute.vitos23.util.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KVClusterImpl implements KVCluster {

    private static final Logger log = LoggerFactory.getLogger(KVClusterImpl.class);

    private final ClusterKVServiceFactory kvServiceFactory;
    private final Map<String, NodePorts> portsByEndpoint;
    /// Dynamic dictionary of started nodes.
    /// The value is created when the node starts and is deleted when it stops.
    /// Unfortunately, this was not a very pretty solution that had to be implemented,
    /// because in tests the node can be started after stopping, which [KVService] prohibits.
    private final Map<String, KVService> kvServiceByEndpoint;

    public KVClusterImpl(ClusterKVServiceFactory kvServiceFactory, List<NodePorts> ports) {
        this.kvServiceFactory = kvServiceFactory;
        this.portsByEndpoint = ports.stream()
                .collect(Collectors.toMap(port -> HttpUtils.getLocalEndpoint(port.httpPort()), Function.identity()));
        this.kvServiceByEndpoint = LinkedHashMap.newLinkedHashMap(ports.size());
    }

    @Override
    public void start() {
        portsByEndpoint.keySet().forEach(this::start);
    }

    @Override
    public void start(String endpoint) {
        NodePorts ports = portsByEndpoint.get(endpoint);
        if (ports == null) {
            throw new IllegalArgumentException("Unknown node");
        }
        if (kvServiceByEndpoint.containsKey(endpoint)) {
            return;
        }
        try {
            KVService node = kvServiceFactory.create(ports);
            node.start();
            log.info("Started node {}", ports);
            kvServiceByEndpoint.put(endpoint, node);
        } catch (IOException e) {
            throw new ServerException(e);
        }
    }

    @Override
    public void stop() {
        portsByEndpoint.keySet().forEach(this::stop);
    }

    @Override
    public void stop(String endpoint) {
        if (!portsByEndpoint.containsKey(endpoint)) {
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
        return List.copyOf(portsByEndpoint.keySet());
    }
}
