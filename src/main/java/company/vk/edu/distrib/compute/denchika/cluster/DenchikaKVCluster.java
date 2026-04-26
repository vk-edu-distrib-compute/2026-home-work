package company.vk.edu.distrib.compute.denchika.cluster;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.denchika.cluster.hashing.DistributingAlgorithm;
import company.vk.edu.distrib.compute.denchika.service.ClusterKVService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DenchikaKVCluster implements KVCluster {
    private final Map<String, KVService> nodes = new ConcurrentHashMap<>();
    private final List<String> endpoints;

    public DenchikaKVCluster(List<Integer> ports, Dao<byte[]> dao, DistributingAlgorithm hasher) {
        this.endpoints = new ArrayList<>();
        for (int port : ports) {
            String endpoint = "http://localhost:" + port;
            endpoints.add(endpoint);
            nodes.put(endpoint, createNode(port, dao, hasher, endpoint));
        }
    }

    @Override
    public void start() {
        for (KVService node : nodes.values()) {
            node.start();
        }
    }

    @Override
    public void start(String endpoint) {
        KVService node = nodes.get(endpoint);
        if (node == null) {
            throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
        }
        node.start();
    }

    @Override
    public void stop() {
        for (KVService node : nodes.values()) {
            node.stop();
        }
    }

    @Override
    public void stop(String endpoint) {
        KVService node = nodes.get(endpoint);
        if (node == null) {
            throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
        }
        node.stop();
    }

    @Override
    public List<String> getEndpoints() {
        return endpoints;
    }

    private KVService createNode(int port, Dao<byte[]> dao, DistributingAlgorithm hasher, String endpoint) {
        return new ClusterKVService(port, dao, hasher, endpoint);
    }
}
