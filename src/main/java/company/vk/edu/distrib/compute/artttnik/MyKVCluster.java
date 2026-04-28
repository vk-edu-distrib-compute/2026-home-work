package company.vk.edu.distrib.compute.artttnik;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.artttnik.shard.ShardingStrategy;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MyKVCluster implements KVCluster {
    private static final Path CLUSTER_STORAGE_ROOT = Path.of("data", "artttnik-cluster");

    private final List<String> endpoints;
    private final Map<String, KVService> servicesByEndpoint;
    private final int replicaCount;

    public MyKVCluster(List<Integer> ports, ShardingStrategy shardingStrategy, int replicaCount) {
        this.endpoints = ports.stream()
                .map(port -> MyReplicatedKVService.formatEndpoint(port, MyReplicatedKVService.defaultGrpcPort(port)))
                .toList();
        this.servicesByEndpoint = new LinkedHashMap<>();
        this.replicaCount = replicaCount;

        for (int i = 0; i < ports.size(); i++) {
            int port = ports.get(i);
            int grpcPort = MyReplicatedKVService.defaultGrpcPort(port);
            String endpoint = endpoints.get(i);
            servicesByEndpoint.put(endpoint, createNodeService(port, grpcPort, shardingStrategy));
        }
    }

    private KVService createNodeService(int port, int grpcPort, ShardingStrategy shardingStrategy) {
        Path nodeStoragePath = CLUSTER_STORAGE_ROOT.resolve(String.valueOf(port));
        return new MyReplicatedKVService(
                port,
                grpcPort,
                createReplicaDaos(nodeStoragePath),
                endpoints,
                shardingStrategy
        );
    }

    private List<Dao<byte[]>> createReplicaDaos(Path nodeStoragePath) {
        List<Dao<byte[]>> result = new ArrayList<>(replicaCount);
        for (int i = 0; i < replicaCount; i++) {
            result.add(new MyDao(nodeStoragePath.resolve("replica-" + i)));
        }

        return Collections.unmodifiableList(result);
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
