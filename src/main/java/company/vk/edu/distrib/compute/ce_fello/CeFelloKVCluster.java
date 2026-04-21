package company.vk.edu.distrib.compute.ce_fello;

import company.vk.edu.distrib.compute.KVCluster;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class CeFelloKVCluster implements KVCluster {
    private final ReentrantLock lifecycleLock = new ReentrantLock();

    private final List<String> endpoints;
    private final Map<String, Path> storageDirectories;
    private final CeFelloKeyDistributor distributor;
    private final Map<String, CeFelloClusterNodeServer> activeNodes = new ConcurrentHashMap<>();

    public CeFelloKVCluster(List<Integer> ports) {
        this(ports, Path.of(".data", "ce_fello", "cluster"), CeFelloDistributionMode.fromSystemProperty());
    }

    CeFelloKVCluster(List<Integer> ports, Path storageRoot, CeFelloDistributionMode distributionMode) {
        Objects.requireNonNull(ports, "ports");
        Objects.requireNonNull(storageRoot, "storageRoot");
        Objects.requireNonNull(distributionMode, "distributionMode");

        List<Integer> sortedPorts = new ArrayList<>(ports);
        sortedPorts.sort(Comparator.naturalOrder());
        validatePorts(sortedPorts);

        this.endpoints = sortedPorts.stream()
                .map(port -> "http://localhost:" + port)
                .toList();
        this.storageDirectories = createStorageDirectories(sortedPorts, storageRoot);
        this.distributor = distributionMode.newDistributor(endpoints);
    }

    @Override
    public void start() {
        lifecycleLock.lock();
        try {
            for (String endpoint : endpoints) {
                if (!activeNodes.containsKey(endpoint)) {
                    startNode(endpoint);
                }
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    @Override
    public void start(String endpoint) {
        lifecycleLock.lock();
        try {
            requireKnownEndpoint(endpoint);
            if (activeNodes.containsKey(endpoint)) {
                return;
            }
            startNode(endpoint);
        } finally {
            lifecycleLock.unlock();
        }
    }

    @Override
    public void stop() {
        lifecycleLock.lock();
        try {
            for (String endpoint : List.copyOf(activeNodes.keySet())) {
                stopNode(endpoint);
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    @Override
    public void stop(String endpoint) {
        lifecycleLock.lock();
        try {
            requireKnownEndpoint(endpoint);
            stopNode(endpoint);
        } finally {
            lifecycleLock.unlock();
        }
    }

    @Override
    public List<String> getEndpoints() {
        return endpoints;
    }

    private void startNode(String endpoint) {
        CeFelloClusterNodeServer nodeServer = createNode(endpoint);
        try {
            nodeServer.start();
            activeNodes.put(endpoint, nodeServer);
        } catch (RuntimeException e) {
            nodeServer.stop();
            throw e;
        }
    }

    private void stopNode(String endpoint) {
        CeFelloClusterNodeServer nodeServer = activeNodes.remove(endpoint);
        if (nodeServer != null) {
            nodeServer.stop();
        }
    }

    private CeFelloClusterNodeServer createNode(String endpoint) {
        try {
            return new CeFelloClusterNodeServer(endpoint, distributor, storageDirectories.get(endpoint));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create node for " + endpoint, e);
        }
    }

    private void requireKnownEndpoint(String endpoint) {
        if (!storageDirectories.containsKey(endpoint)) {
            throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
        }
    }

    private static Map<String, Path> createStorageDirectories(List<Integer> sortedPorts, Path storageRoot) {
        Map<String, Path> result = new ConcurrentHashMap<>();
        for (Integer port : sortedPorts) {
            result.put("http://localhost:" + port, storageRoot.resolve(String.valueOf(port)));
        }
        return Map.copyOf(result);
    }

    private static void validatePorts(List<Integer> ports) {
        if (ports.isEmpty()) {
            throw new IllegalArgumentException("Missing ports for the cluster");
        }

        Integer previousPort = null;
        for (Integer port : ports) {
            if (port == null || port <= 0) {
                throw new IllegalArgumentException("Port must be positive");
            }
            if (previousPort != null && previousPort.equals(port)) {
                throw new IllegalArgumentException("Duplicate port: " + port);
            }
            previousPort = port;
        }
    }
}
