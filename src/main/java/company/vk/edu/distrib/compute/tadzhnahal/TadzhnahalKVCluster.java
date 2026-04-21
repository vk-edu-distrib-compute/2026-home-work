package company.vk.edu.distrib.compute.tadzhnahal;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TadzhnahalKVCluster implements KVCluster {
    private static final String LOCALHOST = "http://localhost:";

    private final TadzhnahalKVServiceFactory factory;
    private final List<String> endpoints;
    private final Map<String, Integer> portsByEndpoint;
    private final Map<String, KVService> startedNodes;

    public TadzhnahalKVCluster(List<Integer> ports) {
        this.factory = new TadzhnahalKVServiceFactory();
        this.endpoints = new ArrayList<>();
        this.portsByEndpoint = new LinkedHashMap<>();
        this.startedNodes = new LinkedHashMap<>();

        for (Integer port : ports) {
            if (port == null) {
                throw new IllegalArgumentException("Port must not be null");
            }

            String endpoint = LOCALHOST + port;
            if (portsByEndpoint.containsKey(endpoint)) {
                throw new IllegalArgumentException("Duplicate endpoint: " + endpoint);
            }

            endpoints.add(endpoint);
            portsByEndpoint.put(endpoint, port);
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
        if (startedNodes.containsKey(endpoint)) {
            return;
        }

        Integer port = portsByEndpoint.get(endpoint);
        if (port == null) {
            throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
        }

        try {
            KVService service = factory.createClusterNode(port, endpoints);
            service.start();
            startedNodes.put(endpoint, service);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot create node for endpoint " + endpoint, e);
        }
    }

    @Override
    public void stop() {
        List<String> activeEndpoints = new ArrayList<>(startedNodes.keySet());
        for (String endpoint : activeEndpoints) {
            stop(endpoint);
        }
    }

    @Override
    public void stop(String endpoint) {
        KVService service = startedNodes.remove(endpoint);
        if (service == null) {
            return;
        }

        service.stop();
    }

    @Override
    public List<String> getEndpoints() {
        return new ArrayList<>(endpoints);
    }
}
