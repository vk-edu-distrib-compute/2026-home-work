package company.vk.edu.distrib.compute.maryarta.replication;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.maryarta.sharding.ConsistentHashing;
import company.vk.edu.distrib.compute.maryarta.sharding.RendezvousHashing;
import company.vk.edu.distrib.compute.maryarta.sharding.ShardingStrategy;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicatedKVClusterImpl implements KVCluster {
    private final List<String> endpoints;
    private final Map<String, KVService> kvServices;
    private final ShardingStrategy shardingStrategy;

    public ReplicatedKVClusterImpl(
            List<Integer> ports,
            ShardingStrategy.ShardingAlgorithm shardingAlgorithm,
            int replicationFactor
    ) {
        this.endpoints = ports.stream()
                .map(port -> "http://localhost:" + port)
                .toList();
        this.shardingStrategy = createShardingStrategy(shardingAlgorithm, endpoints);
        this.kvServices = createServices(ports, replicationFactor);
    }

    private Map<String, KVService> createServices(List<Integer> ports, int replicationFactor) {
        Map<String, KVService> services = new ConcurrentHashMap<>();

        for (int port : ports) {
            String endpoint = "http://localhost:" + port;
            services.put(endpoint, createService(port, replicationFactor));
        }

        return services;
    }

    private ReplicatedService createService(int port, int replicationFactor) {
        try {
            return new ReplicatedServiceImpl(
                    port,
                    shardingStrategy,
                    replicationFactor,
                    endpoints
            );
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create service for port " + port, e);
        }
    }

    private static ShardingStrategy createShardingStrategy(
            ShardingStrategy.ShardingAlgorithm shardingAlgorithm,
            List<String> endpoints
    ) {
        return switch (shardingAlgorithm) {
            case CONSISTENT -> new ConsistentHashing(endpoints);
            case RENDEZVOUS -> new RendezvousHashing(endpoints);
        };
    }

    @Override
    public void start() {
        for (String endpoint : endpoints) {
            start(endpoint);
        }
    }

    @Override
    public void start(String endpoint) {
        KVService service = serviceByEndpoint(endpoint);

        try {
            service.start();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start node: " + endpoint, e);
        }
    }

    @Override
    public void stop() {
        for (String endpoint : endpoints) {
            stop(endpoint);
        }
    }

    @Override
    public void stop(String endpoint) {
        serviceByEndpoint(endpoint).stop();
    }

    @Override
    public List<String> getEndpoints() {
        return endpoints;
    }

    private KVService serviceByEndpoint(String endpoint) {
        KVService service = kvServices.get(endpoint);

        if (service == null) {
            throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
        }

        return service;
    }
}
