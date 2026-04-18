package company.vk.edu.distrib.compute.nst1610;

import company.vk.edu.distrib.compute.KVCluster;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Nst1610KVCluster implements KVCluster {
    private final List<String> endpoints;
    private final Map<String, Nst1610KVService> nodes = new ConcurrentHashMap<>();

    public Nst1610KVCluster(List<Integer> ports) {
        this.endpoints = ports.stream()
            .map(port -> "http://localhost:" + port)
            .toList();
    }

    @Override
    public synchronized void start() {
        for (String endpoint : endpoints) {
            start(endpoint);
        }
    }

    @Override
    public synchronized void start(String endpoint) {
        try {
            int port = URI.create(endpoint).getPort();
            Nst1610KVService service = new Nst1610KVService(port, endpoints, endpoint);
            nodes.put(endpoint, service);
            refreshClusterEndpoints();
            service.start();
        } catch (IOException e) {
            nodes.remove(endpoint);
            throw new IllegalStateException("Failed to start node " + endpoint, e);
        }
    }

    @Override
    public synchronized void stop() {
        for (String endpoint : List.copyOf(nodes.keySet())) {
            stop(endpoint);
        }
    }

    @Override
    public synchronized void stop(String endpoint) {
        Nst1610KVService service = nodes.remove(endpoint);
        if (service != null) {
            refreshClusterEndpoints();
            service.stop();
        }
    }

    @Override
    public List<String> getEndpoints() {
        return endpoints;
    }

    private void refreshClusterEndpoints() {
        List<String> activeEndpoints = List.copyOf(nodes.keySet());
        for (Nst1610KVService service : nodes.values()) {
            service.updateClusterEndpoints(activeEndpoints);
        }
    }
}
