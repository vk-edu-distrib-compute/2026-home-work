package company.vk.edu.distrib.compute.che1nov;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.che1nov.cluster.ClusterGrpcServer;
import company.vk.edu.distrib.compute.che1nov.cluster.ClusterProxyClient;
import company.vk.edu.distrib.compute.che1nov.cluster.ShardRouter;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

public class KVClusterImpl implements KVCluster {
    private final List<String> endpoints;
    private final Map<String, ClusterNode> nodes;
    private final Set<String> startedEndpoints;
    private final ShardRouter shardRouter;
    private final ClusterProxyClient proxyClient;
    private final ReentrantLock lifecycleLock;
    private final int replicationFactor;

    public KVClusterImpl(List<Integer> ports, String shardingAlgorithm) {
        this(ports, shardingAlgorithm, 1);
    }

    public KVClusterImpl(List<Integer> ports, String shardingAlgorithm, int replicationFactor) {
        validatePorts(ports);

        this.endpoints = toEndpoints(ports);
        this.nodes = new LinkedHashMap<>();
        this.startedEndpoints = new LinkedHashSet<>();
        this.shardRouter = KVClusterFactoryImpl.createRouter(endpoints, shardingAlgorithm);
        this.proxyClient = new ClusterProxyClient();
        this.lifecycleLock = new ReentrantLock();
        this.replicationFactor = validateReplicationFactor(replicationFactor, ports.size());

        initializeNodes(ports);
    }

    @Override
    public void start() {
        lifecycleLock.lock();
        try {
            for (String endpoint : endpoints) {
                startInternal(endpoint);
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    @Override
    public void start(String endpoint) {
        lifecycleLock.lock();
        try {
            startInternal(endpoint);
        } finally {
            lifecycleLock.unlock();
        }
    }

    @Override
    public void stop() {
        lifecycleLock.lock();
        try {
            for (String endpoint : endpoints) {
                stopInternal(endpoint);
            }
            proxyClient.close();
        } finally {
            lifecycleLock.unlock();
        }
    }

    @Override
    public void stop(String endpoint) {
        lifecycleLock.lock();
        try {
            stopInternal(endpoint);
        } finally {
            lifecycleLock.unlock();
        }
    }

    private void startInternal(String endpoint) {
        ClusterNode node = getNode(endpoint);
        if (startedEndpoints.contains(endpoint)) {
            return;
        }

        node.start();
        startedEndpoints.add(endpoint);
    }

    private void stopInternal(String endpoint) {
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

    private void initializeNodes(List<Integer> ports) {
        for (int port : ports) {
            int grpcPort = mapGrpcPort(port);
            String endpoint = endpoint(port, grpcPort);
            nodes.put(endpoint, createClusterNode(port, grpcPort, endpoint));
        }
    }

    private ClusterNode createClusterNode(int port, int grpcPort, String endpoint) {
        Path dataPath = Path.of(".data", "node-" + port);
        return new ClusterNode(port, grpcPort, endpoint, dataPath);
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
            result.add(endpoint(port, mapGrpcPort(port)));
        }
        return result;
    }

    private static String endpoint(int port, int grpcPort) {
        return "http://localhost:" + port + "?grpcPort=" + grpcPort;
    }

    private static int mapGrpcPort(int httpPort) {
        if (httpPort <= 32_767) {
            return httpPort + 32_768;
        }
        return httpPort - 32_768;
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

    private static int validateReplicationFactor(int replicationFactor, int clusterSize) {
        if (replicationFactor <= 0) {
            throw new IllegalArgumentException("replicationFactor must be positive");
        }
        if (replicationFactor > clusterSize) {
            throw new IllegalArgumentException("replicationFactor must not be greater than cluster size");
        }
        return replicationFactor;
    }

    private final class ClusterNode {
        private final int port;
        private final int grpcPort;
        private final String endpoint;
        private final Path dataPath;
        private KVService service;
        private ClusterGrpcServer grpcServer;

        private ClusterNode(int port, int grpcPort, String endpoint, Path dataPath) {
            this.port = port;
            this.grpcPort = grpcPort;
            this.endpoint = endpoint;
            this.dataPath = dataPath;
        }

        private void start() {
            try {
                FSDao dao = new FSDao(dataPath);
                KVServiceImpl kvService = new KVServiceImpl(
                        port,
                        dao,
                        endpoint,
                        shardRouter,
                        proxyClient,
                        replicationFactor
                );
                grpcServer = new ClusterGrpcServer(grpcPort, kvService);
                grpcServer.start();
                service = kvService;
                service.start();
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException("Failed to initialize node on port " + port, e);
            }
        }

        @SuppressWarnings("PMD.UseTryWithResources")
        private void stop() {
            try {
                if (service != null) {
                    service.stop();
                }
            } finally {
                if (grpcServer != null) {
                    grpcServer.close();
                }
            }
        }
    }
}
