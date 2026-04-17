package company.vk.edu.distrib.compute.nst1610;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import java.io.IOException;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Nst1610KVCluster implements KVCluster {
    private final List<String> endpoints;
    private final Map<String, KVService> nodes = new LinkedHashMap<>();

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
            KVService service = new Nst1610KVService(port, endpoints, endpoint);
            service.start();
            nodes.put(endpoint, service);
        } catch (IOException e) {
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
        KVService service = nodes.remove(endpoint);
        if (service != null) {
            service.stop();
        }
    }

    @Override
    public List<String> getEndpoints() {
        return endpoints;
    }
}
