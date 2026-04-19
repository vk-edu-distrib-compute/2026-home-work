package company.vk.edu.distrib.compute.yyaarr;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class YyaarrKVCluster implements KVCluster {
    private static final Logger LOGGER = LoggerFactory.getLogger(YyaarrKVCluster.class);

    private final Map<Integer, KVService> nodes = new ConcurrentHashMap<>();
    private final List<Integer> ports;
    private final YyaarrKVServiceFileFactory factory = new YyaarrKVServiceFileNodeFactory(this);

    YyaarrKVCluster(List<Integer> ports) {
        this.ports = new ArrayList<>(ports);
        Collections.sort(this.ports);

        for (int port : this.ports) {
            try {
               KVService node = factory.create(port);
            nodes.put(port, node);
            LOGGER.info("Created node on port {}", port);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void start() {
        for (KVService node : nodes.values()) {
            node.start();
        }
        if (LOGGER.isInfoEnabled()) {
        LOGGER.info("Cluster started with {} nodes", nodes.size());
        }
    }

    @Override
    public void stop() {
        for (KVService node : nodes.values()) {
            node.stop();
        }
        LOGGER.info("Cluster stopped");
    }

    @Override
    public List<String> getEndpoints() {
        List<String> endpoints = new ArrayList<>();
        for (int port : ports) {
            endpoints.add("http://localhost:" + port);
        }
        return endpoints;
    }

    @Override
    public void start(String endpoint) {
        int port = extractPort(endpoint);
        KVService node = nodes.get(port);
        if (node != null) {
            node.start();
            LOGGER.info("Started node on port {}", port);
        }
    }

    @Override
    public void stop(String endpoint) {
        int port = extractPort(endpoint);
        KVService node = nodes.get(port);
        if (node != null) {
            node.stop();
            LOGGER.info("Stopped node on port {}", port);
        }
    }

    //получения порта узла
    public int getPortForKey(String key) {
        if (ports.isEmpty()) {
            throw new IllegalStateException("No nodes in cluster");
        }
        int hash = Math.abs(key.hashCode());
        int nodeIndex = hash % ports.size();
        return ports.get(nodeIndex);
    }

    private int extractPort(String endpoint) {
        String[] parts = endpoint.split(":");
        return Integer.parseInt(parts[2]);
    }
}


