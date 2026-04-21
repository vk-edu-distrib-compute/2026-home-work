package company.vk.edu.distrib.compute.maryarta;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KVClusterImpl implements KVCluster {
    private final List<String> endpoints;
    private final Map<String, KVService> kvServices = new ConcurrentHashMap<>();
    ShardingStrategy shardingStrategy;

    public KVClusterImpl(List<Integer> ports, ShardingStrategy.ShardingAlgorithm shardingAlgorithm) {
        this.endpoints = ports.stream().map(port -> "http://localhost:" + port).toList();
        switch (shardingAlgorithm){
            case CONSISTENT -> shardingStrategy = new ConsistentHashing(endpoints);
            case RENDEZVOUS -> shardingStrategy = new RendezvousHashing(endpoints);
        }
        for (Integer port: ports) {
            try {
                kvServices.put("http://localhost:" + port, new ShardedKVServiceImpl(port, shardingStrategy));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
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
            throw new RuntimeException(e);
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
