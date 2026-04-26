package company.vk.edu.distrib.compute.denchika.cluster;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.denchika.cluster.hashing.DistributingAlgorithm;
import company.vk.edu.distrib.compute.denchika.service.ClusterKVService;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class DenchikaKVCluster implements KVCluster {

    private static final String PREFIX = "http://localhost:";

    private final Map<String, KVService> nodes = new ConcurrentHashMap<>();
    private final List<String> endpoints = new ArrayList<>();

    public DenchikaKVCluster(
            List<Integer> ports,
            DistributingAlgorithm algorithm,
            Supplier<Dao<byte[]>> daoSupplier
    ) {

        for (int port : ports) {
            endpoints.add(PREFIX + port);
        }

        for (int port : ports) {
            String endpoint = PREFIX + port;

            nodes.put(endpoint, new ClusterKVService(
                    port,
                    daoSupplier.get(),
                    algorithm,
                    endpoints
            ));
        }
    }

    @Override
    public void start() {
        endpoints.forEach(this::start);
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
        endpoints.forEach(this::stop);
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
}
