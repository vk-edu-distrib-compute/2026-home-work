package company.vk.edu.distrib.compute.gavrilova_ekaterina.replication;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicationKVClusterImpl implements KVCluster {

    private static final String LOCALHOST = "http://localhost:";
    private final List<String> endpoints;
    private final Map<String, ReplicationKVServiceImpl> nodesByEndpoints = new ConcurrentHashMap<>();
    private final int replicationFactor;

    public ReplicationKVClusterImpl(List<Integer> ports) {
        this.endpoints = ports.stream()
                .map(this::toEndpoint)
                .toList();
        this.replicationFactor = ReplicationConfigUtils.getReplicationFactor();
        if (replicationFactor > ports.size()) {
            throw new IllegalArgumentException("replicationFactor > cluster size");
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
        ReplicationKVServiceImpl node = nodesByEndpoints.get(endpoint);

        if (node == null) {
            int port = parsePort(endpoint);

            try {
                node = new ReplicationKVServiceImpl(port, endpoints, replicationFactor);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to start KV node at endpoint: " + endpoint, e);
            }
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
