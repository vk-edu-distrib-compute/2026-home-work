package company.vk.edu.distrib.compute.ronshinvsevolod;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RendezvousHashKVCluster implements KVCluster {
    private final List<KVService> nodes = new ArrayList<>();
    private final List<Integer> ports;
    private final List<String> endpoints;
    private final Map<String, KVService> endpointToNode = new ConcurrentHashMap<>();

    public RendezvousHashKVCluster(List<Integer> ports) {
        this.ports = ports;
        this.endpoints = ports.stream()
                .map(port -> "http://localhost:" + port)
                .collect(Collectors.toList());
    }

    private KVService createNode(int port, List<String> endpoints) throws IOException {
        Dao<byte[]> dao = new FileDao("./data/rendezvous_node_" + port);
        // codacy:ignore=avoid_instantiating_objects_in_loops
        HashStrategy strategy = new RendezvousHashStrategy(endpoints);
        // codacy:ignore=avoid_instantiating_objects_in_loops
        return new ShardedKVService(dao, port, strategy);
    }

    @Override
    public void start() {
        for (int port : ports) {
            try {
                KVService shardedService = createNode(port, endpoints);
                shardedService.start();
                nodes.add(shardedService);
                String endpoint = "http://localhost:" + port;
                endpointToNode.put(endpoint, shardedService);
            } catch (IOException e) {
            throw new IllegalStateException("Failed to start node on port " + port, e);
            // codacy:ignore=avoid_instantiating_objects_in_loops
            }
        }
    }

    @Override
    public void start(String endpoint) {
        KVService node = endpointToNode.get(endpoint);
        if (node != null) {
            node.start();
        }
    }

    @Override
    public void stop() {
        for (KVService node : nodes) {
            node.stop();
        }
        nodes.clear();
    }

    @Override
    public void stop(String endpoint) {
        KVService node = endpointToNode.get(endpoint);
        if (node != null) {
            node.stop();
        }
    }

    @Override
    public List<String> getEndpoints() {
        return endpoints;
    }
}
