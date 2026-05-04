package company.vk.edu.distrib.compute.nihuaway00;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.nihuaway00.sharding.ShardingStrategy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class NihuawayKVCluster implements KVCluster {
    private final ShardingStrategy shardingStrategy;
    private final Map<String, KVService> services = new ConcurrentHashMap<>();
    private final KVServiceFactory serviceFactory;

    public NihuawayKVCluster(ShardingStrategy shardingStrategy, KVServiceFactory serviceFactory) {
        this.shardingStrategy = shardingStrategy;
        this.serviceFactory = serviceFactory;
    }

    @Override
    public void start() {
        List<String> endpoints = getEndpoints();
        endpoints.forEach(this::start);
    }

    @Override
    public void start(String endpoint) {
        int port = extractPort(endpoint);

        KVService service = services.computeIfAbsent(endpoint, (k) -> {
            try {
                return serviceFactory.create(port);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        service.start();
        shardingStrategy.enableNode(endpoint);
    }

    @Override
    public void stop() {
        List<String> endpoints = getEndpoints();
        endpoints.forEach(this::stop);
    }

    @Override
    public void stop(String endpoint) {
        services.get(endpoint).stop();
        shardingStrategy.disableNode(endpoint);
    }

    @Override
    public List<String> getEndpoints() {
        return shardingStrategy.getEndpoints();
    }

    private int extractPort(String endpoint) {
        String[] parts = endpoint.split(":");
        return Integer.parseInt(parts[parts.length - 1]);
    }
}
