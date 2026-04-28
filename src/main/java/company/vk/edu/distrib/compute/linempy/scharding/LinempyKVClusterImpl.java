package company.vk.edu.distrib.compute.linempy.scharding;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static company.vk.edu.distrib.compute.linempy.scharding.GrpcConfigUtils.GRPC_PORT_SHIFT;

/**
 * Управление кластером: запуск и остановка всех нод.
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
        KVService node = nodes.get(endpoint);
        if (node != null) {
            node.stop();
        }
    }

    @Override
    public List<String> getEndpoints() {
        return endpoints.stream()
                .map(this::addGrpcPortToEndpoint)
                .collect(Collectors.toList());
    }

    private String addGrpcPortToEndpoint(String httpEndpoint) {
        int httpPort = extractPort(httpEndpoint);
        int grpcPort = httpPort + GRPC_PORT_SHIFT;
        return httpEndpoint + "?grpcPort=" + grpcPort;
    }

    private int extractPort(String endpoint) {
        return Integer.parseInt(endpoint.split(":")[2]);
    }

    private KVService getNode(String endpoint) {
        String cleanEndpoint = endpoint.split("\\?")[0];
        KVService node = nodes.get(cleanEndpoint);
        if (node == null) {
            throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
        }
        return node;
    }
}
