package company.vk.edu.distrib.compute.glekoz.cluster;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class KVServiceClusterGK implements KVCluster {

    private static final String LOCALHOST = "http://localhost:";
    private final List<String> endpoints;
    private final Map<String, KVClusteredServiceGK> nodesByEndpoints = new ConcurrentHashMap<>();
    private final ConsistentHash discovery;

    public KVServiceClusterGK(List<Integer> ports) {
        this.endpoints = ports.stream()
                .map(this::toEndpoint)
                .toList();
        this.discovery = new ConsistentHash(endpoints);
    }

    @Override
    public void start() {
        for (String endpoint : endpoints) {
            start(endpoint);
        }
    }

    @Override
    public void start(String endpoint) {
        KVClusteredServiceGK node = nodesByEndpoints.get(endpoint);

        if (node == null) {
            int port = parsePort(endpoint);
            node = new KVClusteredServiceGK(port, discovery);
            nodesByEndpoints.put(endpoint, node);
        }

        node.start();
    }

    @Override
    public void stop() {
        for (KVService node : nodesByEndpoints.values()) {
            node.stop();
        }
        nodesByEndpoints.clear();
    }

    @Override
    public void stop(String endpoint) {
        KVService node = nodesByEndpoints.remove(endpoint);
        if (node != null) {
            node.stop();
        }
    }

    @Override
    public List<String> getEndpoints() {
        return List.copyOf(endpoints);
    }

    private String toEndpoint(int port) {
        return LOCALHOST + port;
    }

    private int parsePort(String endpoint) {
        return Integer.parseInt(endpoint.substring(LOCALHOST.length()));
    }

}
