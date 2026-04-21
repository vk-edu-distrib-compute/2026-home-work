package company.vk.edu.distrib.compute.martinez1337.service;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Martinez1337KVCluster implements KVCluster {

    public static final String BASE_URL = "http://localhost:";

    private final List<String> endpoints = new ArrayList<>();
    private final Map<String, KVService> runningServices = new ConcurrentHashMap<>();
    private final KVServiceFactory serviceFactory = new Martinez1337KVServiceFactory();

    public Martinez1337KVCluster(List<Integer> ports) {
        for (Integer port : ports) {
            endpoints.add(BASE_URL + port);
        }
    }

    @Override
    public void start() {
        for (String endpoint : endpoints) {
            startNode(endpoint);
        }
    }

    @Override
    public void start(String endpoint) {
        startNode(endpoint);
    }

    @Override
    public void stop() {
        for (KVService service : runningServices.values()) {
            service.stop();
        }
        runningServices.clear();
    }

    @Override
    public void stop(String endpoint) {
        KVService service = runningServices.remove(endpoint);
        if (service == null) {
            throw new IllegalArgumentException("Endpoint not running: " + endpoint);
        }
        service.stop();
    }

    @Override
    public List<String> getEndpoints() {
        return List.copyOf(endpoints);
    }

    private void startNode(String endpoint) {
        if (!endpoints.contains(endpoint)) {
            throw new IllegalArgumentException("Endpoint not found: " + endpoint);
        }
        if (runningServices.containsKey(endpoint)) {
            return;
        }

        int port = extractPort(endpoint);
        int nodeId = endpoints.indexOf(endpoint);
        try {
            KVService service = serviceFactory.create(port);
            if (service instanceof ClusterAwareKVService clusterAware) {
                clusterAware.setCluster(endpoints, nodeId);
            }
            service.start();
            runningServices.put(endpoint, service);
        } catch (IOException e) {
            throw new RuntimeException("Failed to start node on port " + port, e);
        }
    }

    private int extractPort(String endpoint) {
        return Integer.parseInt(endpoint.substring(endpoint.lastIndexOf(':') + 1));
    }
}
