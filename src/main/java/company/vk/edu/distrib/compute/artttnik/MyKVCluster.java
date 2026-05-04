package company.vk.edu.distrib.compute.artttnik;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.artttnik.shard.ShardingStrategy;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MyKVCluster implements KVCluster {
    private static final Path CLUSTER_STORAGE_ROOT = Path.of("data", "artttnik-cluster");

    private final List<String> endpoints;
    private final Map<String, KVService> servicesByEndpoint;

    public MyKVCluster(List<Integer> ports, ShardingStrategy shardingStrategy) {
        this.endpoints = ports.stream()
                .map(port -> "http://localhost:" + port)
                .toList();
        this.servicesByEndpoint = new LinkedHashMap<>();

        for (int i = 0; i < ports.size(); i++) {
            int port = ports.get(i);
            String endpoint = endpoints.get(i);
            servicesByEndpoint.put(endpoint, createNodeService(port, shardingStrategy));
        }
    }

    private KVService createNodeService(int port, ShardingStrategy shardingStrategy) {
        Path nodeStoragePath = CLUSTER_STORAGE_ROOT.resolve(String.valueOf(port));
        return new MyKVService(port, new MyDao(nodeStoragePath), endpoints, shardingStrategy);
    }

    @Override
    public void start() {
        for (KVService service : servicesByEndpoint.values()) {
            service.start();
        }
    }

    @Override
    public void start(String endpoint) {
        resolveService(endpoint).start();
    }

    @Override
    public void stop() {
        for (KVService service : servicesByEndpoint.values()) {
            service.stop();
        }
    }

    @Override
    public void stop(String endpoint) {
        resolveService(endpoint).stop();
    }

    @Override
    public List<String> getEndpoints() {
        return new ArrayList<>(endpoints);
    }

    private KVService resolveService(String endpoint) {
        KVService service = servicesByEndpoint.get(endpoint);
        if (service == null) {
            throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
        }

        return service;
    }
}
