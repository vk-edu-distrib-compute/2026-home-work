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
    private final Map<String, KVService> kvServices = new ConcurrentHashMap<>();
    ShardingStrategy shardingStrategy;

    public ReplicatedKVClusterImpl(List<Integer> ports, ShardingStrategy.ShardingAlgorithm shardingAlgorithm, int replicationFactor) {
        this.endpoints = ports.stream().map(port -> "http://localhost:" + port).toList();

        shardingStrategy = switch (shardingAlgorithm) {
            case CONSISTENT -> new ConsistentHashing(endpoints);
            case RENDEZVOUS -> new RendezvousHashing(endpoints);
        };

        List<String> endpoints = ports.stream().map(port ->"http://localhost:" + port).toList();

        for (Integer port: ports) {
            String endpoint = "http://localhost:" + port;
            kvServices.put(endpoint, createService(port, replicationFactor, endpoints));
        }
    }

    private ReplicatedService createService(int port, int replicationFactor, List<String> endpoints) {
        try {
            return new ReplicatedServiceImpl(port, shardingStrategy, replicationFactor, endpoints);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create service for port " + port, e);
        }
    }

    @Override
    public void start() {
        for (String endpoint: endpoints) {
            start(endpoint);
        }
    }

    @Override
    public void start(String endpoint) {
        try {
            kvServices.get(endpoint).start();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start node: " + endpoint, e);
        }
    }

    @Override
    public void stop() {
        for (String endpoint: endpoints) {
            stop(endpoint);
        }
    }

    @Override
    public void stop(String endpoint) {
        kvServices.get(endpoint).stop();
    }

    @Override
    public List<String> getEndpoints() {
        return endpoints;
    }
}