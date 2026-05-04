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
    private static final int GRPC_PORT_SHIFT = 32_768;
    private static final int MIN_PORT = 1;
    private static final int MAX_PORT = 65_535;

    private final List<String> endpoints;
    private final Map<String, ClusterNode> nodes;
    private final Map<Integer, Integer> grpcPortsByHttpPort;
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
        this.grpcPortsByHttpPort = assignGrpcPorts(ports);

        this.endpoints = toEndpoints(ports, grpcPortsByHttpPort);
        this.nodes = new LinkedHashMap<>();
        this.startedEndpoints = new LinkedHashSet<>();
        this.shardRouter = KVClusterFactoryImpl.createRouter(endpoints, shardingAlgorithm);
        this.proxyClient = new ClusterProxyClient();
        this.lifecycleLock = new ReentrantLock();
        this.replicationFactor = validateReplicationFactor(replicationFactor, ports.size());

        initializeNodes(ports, grpcPortsByHttpPort);
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
            RuntimeException firstFailure = null;
            for (String endpoint : endpoints) {
                try {
                    stopInternal(endpoint);
                } catch (RuntimeException e) {
                    if (firstFailure == null) {
                        firstFailure = e;
                    }
                }
            }
            proxyClient.close();
            if (firstFailure != null) {
                throw firstFailure;
            }
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

    private void initializeNodes(List<Integer> ports, Map<Integer, Integer> grpcPortByHttpPort) {
        for (int port : ports) {
            int grpcPort = grpcPortByHttpPort.get(port);
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

    private static List<String> toEndpoints(List<Integer> ports, Map<Integer, Integer> grpcPortsByHttpPort) {
        List<String> result = new ArrayList<>(ports.size());
        for (int port : ports) {
            result.add(endpoint(port, grpcPortsByHttpPort.get(port)));
        }
        return result;
    }

    private static String endpoint(int port, int grpcPort) {
        return "http://localhost:" + port + "?grpcPort=" + grpcPort;
    }

    private static int mapGrpcPort(int httpPort) {
        if (httpPort < GRPC_PORT_SHIFT) {
            return httpPort + GRPC_PORT_SHIFT;
        }
        return httpPort - GRPC_PORT_SHIFT;
    }

    @SuppressWarnings("PMD.UseConcurrentHashMap")
    private static Map<Integer, Integer> assignGrpcPorts(List<Integer> httpPorts) {
        Map<Integer, Integer> grpcByHttp = new LinkedHashMap<>();
        Set<Integer> usedPorts = new LinkedHashSet<>(httpPorts);

        for (int httpPort : httpPorts) {
            int grpcPort = mapGrpcPort(httpPort);
            while (usedPorts.contains(grpcPort)) {
                grpcPort = nextPort(grpcPort);
            }
            grpcByHttp.put(httpPort, grpcPort);
            usedPorts.add(grpcPort);
        }
        return grpcByHttp;
    }

    private static int nextPort(int port) {
        if (port >= MAX_PORT) {
            return MIN_PORT;
        }
        return port + 1;
    }

    private static void validatePorts(List<Integer> ports) {
        if (ports == null || ports.isEmpty()) {
            throw new IllegalArgumentException("ports must not be null or empty");
        }

        Set<Integer> unique = new LinkedHashSet<>();
        for (Integer port : ports) {
            if (port == null || port < MIN_PORT || port > MAX_PORT) {
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
            FSDao dao = null;
            KVServiceImpl kvService = null;
            ClusterGrpcServer createdGrpcServer = null;
            try {
                dao = new FSDao(dataPath);
                kvService = new KVServiceImpl(
                        port,
                        dao,
                        endpoint,
                        shardRouter,
                        proxyClient,
                        replicationFactor
                );
                createdGrpcServer = new ClusterGrpcServer(grpcPort, kvService);
                createdGrpcServer.start();
                kvService.start();

                grpcServer = createdGrpcServer;
                service = kvService;
            } catch (java.io.IOException e) {
                cleanupFailedStart(kvService, createdGrpcServer, dao);
                throw new java.io.UncheckedIOException("Failed to initialize node on port " + port, e);
            } catch (RuntimeException e) {
                cleanupFailedStart(kvService, createdGrpcServer, dao);
                throw e;
            }
        }

        private void cleanupFailedStart(KVServiceImpl kvService, ClusterGrpcServer createdGrpcServer, FSDao dao) {
            if (kvService != null) {
                try {
                    kvService.stop();
                } catch (RuntimeException ignored) {
                    // Best effort cleanup after partial startup.
                }
            }
            if (createdGrpcServer != null) {
                try {
                    createdGrpcServer.close();
                } catch (RuntimeException ignored) {
                    // Best effort cleanup after partial startup.
                }
            }
            if (dao != null) {
                try {
                    dao.close();
                } catch (RuntimeException ignored) {
                    // Best effort cleanup after partial startup.
                }
            }
        }

        @SuppressWarnings("PMD.UseTryWithResources")
        private void stop() {
            RuntimeException firstFailure = null;
            try {
                if (service != null) {
                    try {
                        service.stop();
                    } catch (RuntimeException e) {
                        firstFailure = e;
                    } finally {
                        service = null;
                    }
                }
            } finally {
                if (grpcServer != null) {
                    try {
                        grpcServer.close();
                    } catch (RuntimeException e) {
                        if (firstFailure == null) {
                            firstFailure = e;
                        }
                    } finally {
                        grpcServer = null;
                    }
                }
                if (firstFailure != null) {
                    throw firstFailure;
                }
            }
        }
    }
}
