package company.vk.edu.distrib.compute.usl;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.usl.sharding.ShardingStrategy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.Map;

public final class UslKVCluster implements KVCluster {
    private final List<String> endpoints;
    private final Map<String, UslNodeServer> nodesByEndpoint;

    public UslKVCluster(List<Integer> ports, ShardingStrategy shardingStrategy) {
        this(
            ports,
            shardingStrategy,
            HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(1))
                .build()
        );
    }

    UslKVCluster(List<Integer> ports, ShardingStrategy shardingStrategy, HttpClient httpClient) {
        this.endpoints = ports.stream()
            .map(UslNodeServer::endpointUrl)
            .toList();
        this.nodesByEndpoint = createNodes(ports, shardingStrategy, httpClient);
    }

    @Override
    public void start() {
        for (String endpoint : endpoints) {
            start(endpoint);
        }
    }

    @Override
    public void start(String endpoint) {
        node(endpoint).start();
    }

    @Override
    public void stop() {
        for (String endpoint : endpoints) {
            stop(endpoint);
        }
    }

    @Override
    public void stop(String endpoint) {
        node(endpoint).stop();
    }

    @Override
    public List<String> getEndpoints() {
        return endpoints;
    }

    private UslNodeServer node(String endpoint) {
        UslNodeServer node = nodesByEndpoint.get(endpoint);
        if (node == null) {
            throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
        }
        return node;
    }

    private static Map<String, UslNodeServer> createNodes(
        List<Integer> ports,
        ShardingStrategy shardingStrategy,
        HttpClient httpClient
    ) {
        Map<String, UslNodeServer> result = new ConcurrentHashMap<>();
        for (int port : ports) {
            result.put(UslNodeServer.endpointUrl(port), createNode(port, shardingStrategy, httpClient));
        }
        return Map.copyOf(result);
    }

    private static UslNodeServer createNode(
        int port,
        ShardingStrategy shardingStrategy,
        HttpClient httpClient
    ) {
        try {
            return new UslNodeServer(
                port,
                new PersistentByteArrayDao(StoragePaths.persistentDataDir(port)),
                shardingStrategy,
                httpClient
            );
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create cluster node for port " + port, e);
        }
    }
}
