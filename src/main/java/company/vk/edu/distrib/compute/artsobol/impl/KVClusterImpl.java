package company.vk.edu.distrib.compute.artsobol.impl;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KVClusterImpl implements KVCluster {

    private final List<String> endpoints;
    private final Map<String, KVService> nodes = new ConcurrentHashMap<>();
    private final Map<String, Integer> ports = new ConcurrentHashMap<>();
    private final Map<String, Path> paths = new ConcurrentHashMap<>();
    private final Path baseDir;

    public KVClusterImpl(List<Integer> ports) {
        this.endpoints = ports.stream().map(port -> "http://localhost:" + port).toList();
        try {
            baseDir = Files.createTempDirectory("vk-cluster-");
            initNodeConfig(ports);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to initialize cluster directory", e);
        }
    }

    @Override
    public void start() {
        for (String endpoint : endpoints) {
            start(endpoint);
        }
    }

    @Override
    public void start(String endpoint) {
        if (!ports.containsKey(endpoint) || nodes.containsKey(endpoint)) {
            return;
        }

        int port = ports.get(endpoint);
        Path path = paths.get(endpoint);
        KVService node = KVServiceImpl.createNode(endpoints, endpoint, port, path);
        node.start();
        nodes.put(endpoint, node);
    }

    @Override
    public void stop() {
        for (String endpoint : endpoints) {
            stop(endpoint);
        }
    }

    @Override
    public void stop(String endpoint) {
        KVService node = nodes.remove(endpoint);
        if (node != null) {
            node.stop();
        }
    }

    @Override
    public List<String> getEndpoints() {
        return endpoints;
    }

    private void initNodeConfig(List<Integer> ports) {
        for (int i = 0; i < ports.size(); i++) {
            String endpoint = endpoints.get(i);
            this.ports.put(endpoint, ports.get(i));
            paths.put(endpoint, baseDir.resolve("node-" + ports.get(i)));
        }
    }
}
