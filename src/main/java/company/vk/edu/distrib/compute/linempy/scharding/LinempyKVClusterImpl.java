package company.vk.edu.distrib.compute.linempy.scharding;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LinempyKVClusterImpl — описание класса.
 *
 * <p>
 * TODO: добавить описание назначения и поведения класса.
 * </p>
 *
 * @author Linempy
 * @since 16.04.2026
 */
public class LinempyKVClusterImpl implements KVCluster {
    private static final Logger log = LoggerFactory.getLogger(LinempyKVClusterImpl.class);

    private final Map<String, KVService> nodes;
    private final List<String> endpoints;

    public LinempyKVClusterImpl(List<String> endpoints, Map<String, KVService> nodes) {
        this.nodes = new ConcurrentHashMap<>(nodes);
        this.endpoints = endpoints;
    }

    @Override
    public void start() {
        endpoints.forEach(this::start);
        log.info("Cluster started: {}", endpoints);
    }

    @Override
    public void start(String endpoint) {
        KVService node = getNode(endpoint);
        node.start();
        log.info("Node started: {}", endpoint);
    }

    @Override
    public void stop() {
        endpoints.forEach(this::stop);
    }

    @Override
    public void stop(String endpoint) {
        nodes.get(endpoint).stop();
    }

    @Override
    public List<String> getEndpoints() {
        return endpoints;
    }

    private KVService getNode(String endpoint) {
        KVService node = nodes.get(endpoint);
        if (node == null) {
            throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
        }
        return node;
    }
}
