package company.vk.edu.distrib.compute.andrey1af.sharding;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.andrey1af.service.Andrey1afKVService;
import company.vk.edu.distrib.compute.andrey1af.service.Andrey1afKVServiceFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Andrey1afKVClusterImpl implements KVCluster {
    private static final String ENDPOINT_PREFIX = "http://localhost:";

    private final Map<String, Andrey1afKVService> nodes = new ConcurrentHashMap<>();
    private final List<String> endpoints;
    private final Andrey1afKVServiceFactory serviceFactory = new Andrey1afKVServiceFactory();
    private final HashRouter hashRouter;

    public Andrey1afKVClusterImpl(List<Integer> ports) {
        this.endpoints = ports.stream()
                .map(port -> ENDPOINT_PREFIX + port)
                .toList();

        this.hashRouter = new RendezvousHash(endpoints);
    }

    private static int portOf(String endpoint) {
        if (!endpoint.startsWith(ENDPOINT_PREFIX)) {
            throw new IllegalArgumentException("Unsupported endpoint: " + endpoint);
        }
        return Integer.parseInt(endpoint.substring(ENDPOINT_PREFIX.length()));
    }

    @Override
    public void start() {
        endpoints.forEach(this::start);
    }

    @Override
    public void start(String endpoint) {
        validateEndpoint(endpoint);
        nodes.computeIfAbsent(endpoint, this::createAndStartNode);
    }

    @Override
    public void stop() {
        endpoints.forEach(this::stop);
    }

    @Override
    public void stop(String endpoint) {
        validateEndpoint(endpoint);
        Andrey1afKVService service = nodes.remove(endpoint);
        if (service != null) {
            service.stop();
        }
    }

    @Override
    public List<String> getEndpoints() {
        return List.copyOf(endpoints);
    }

    private Andrey1afKVService createAndStartNode(String endpoint) {
        try {
            int port = portOf(endpoint);
            Andrey1afKVService service = serviceFactory.createClusterNode(port, endpoint, hashRouter);
            service.start();
            return service;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start node " + endpoint, e);
        }
    }

    private void validateEndpoint(String endpoint) {
        if (!endpoints.contains(endpoint)) {
            throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
        }
    }
}
