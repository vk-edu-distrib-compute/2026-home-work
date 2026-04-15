package company.vk.edu.distrib.compute.che1nov;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.che1nov.cluster.ClusterProxyClient;
import company.vk.edu.distrib.compute.che1nov.cluster.ShardRouter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KVClusterImpl implements KVCluster {
    private final List<String> endpoints;
    private final Map<String, ClusterNode> nodes;
    private final Set<String> startedEndpoints;
    private final ShardRouter shardRouter;
    private final ClusterProxyClient proxyClient;

    public KVClusterImpl(List<Integer> ports, String shardingAlgorithm) {
        validatePorts(ports);

        this.endpoints = toEndpoints(ports);
        this.nodes = new LinkedHashMap<>();
        this.startedEndpoints = new LinkedHashSet<>();
        this.shardRouter = KVClusterFactoryImpl.createRouter(endpoints, shardingAlgorithm);
        this.proxyClient = new ClusterProxyClient();

        for (int port : ports) {
            String endpoint = endpoint(port);
            nodes.put(endpoint, new ClusterNode(port, endpoint, new InMemoryDao()));
        }
    }

    @Override
    public synchronized void start() {
        for (String endpoint : endpoints) {
            start(endpoint);
        }
    }

    @Override
    public synchronized void start(String endpoint) {
        ClusterNode node = getNode(endpoint);
        if (startedEndpoints.contains(endpoint)) {
            return;
        }

        node.start();
        startedEndpoints.add(endpoint);
    }

    @Override
    public synchronized void stop() {
        for (String endpoint : endpoints) {
            stop(endpoint);
        }
    }

    @Override
    public synchronized void stop(String endpoint) {
        ClusterNode node = getNode(endpoint);
        if (!startedEndpoints.contains(endpoint)) {
            return;
        }

        try {
            node.stop();
        } finally {
            startedEndpoints.remove(endpoint);
        }
    }

    @Override
    public List<String> getEndpoints() {
        return List.copyOf(endpoints);
    }

    private ClusterNode getNode(String endpoint) {
        ClusterNode node = nodes.get(endpoint);
        if (node == null) {
            throw new IllegalArgumentException("Unknown cluster endpoint: " + endpoint);
        }
        return node;
    }

    private static List<String> toEndpoints(List<Integer> ports) {
        List<String> result = new ArrayList<>(ports.size());
        for (int port : ports) {
            result.add(endpoint(port));
        }
        return result;
    }

    private static String endpoint(int port) {
        return "http://localhost:" + port;
    }

    private static void validatePorts(List<Integer> ports) {
        if (ports == null || ports.isEmpty()) {
            throw new IllegalArgumentException("ports must not be null or empty");
        }

        Set<Integer> unique = new LinkedHashSet<>();
        for (Integer port : ports) {
            if (port == null || port <= 0 || port > 65_535) {
                throw new IllegalArgumentException("invalid port: " + port);
            }
            if (!unique.add(port)) {
                throw new IllegalArgumentException("duplicate port: " + port);
            }
        }
    }

    private final class ClusterNode {
        private final int port;
        private final String endpoint;
        private final InMemoryDao dao;
        private KVService service;

        private ClusterNode(int port, String endpoint, InMemoryDao dao) {
            this.port = port;
            this.endpoint = endpoint;
            this.dao = dao;
        }

        private void start() {
            try {
                service = new KVServiceImpl(port, dao, endpoint, shardRouter, proxyClient);
                service.start();
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException("Failed to initialize node on port " + port, e);
            }
        }

        private void stop() {
            if (service != null) {
                service.stop();
                service = null;
            }
        }
    }
}
