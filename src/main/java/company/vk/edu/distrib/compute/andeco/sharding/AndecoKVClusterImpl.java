package company.vk.edu.distrib.compute.andeco.sharding;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.andeco.FileDao;
import company.vk.edu.distrib.compute.andeco.ServerConfigConstants;
import company.vk.edu.distrib.compute.andeco.consistent_hash.ConsistentHash;
import company.vk.edu.distrib.compute.andeco.consistent_hash.MD5HashFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AndecoKVClusterImpl implements KVCluster {
    private final Map<String, Integer> endpointToPort;
    private final Map<String, KVService> endpointToService;
    private final ShardingStrategy<String> shardingType;

    public AndecoKVClusterImpl(List<Integer> ports) {
        this.endpointToPort = new ConcurrentHashMap<>();
        ports.forEach(port -> endpointToPort.put(ServerConfigConstants.LOCALHOST + port, port));
        this.endpointToService = new ConcurrentHashMap<>();
        List<String> endpoints = new ArrayList<>(endpointToPort.keySet());
        var nodes = endpoints.stream()
                .map(e -> new Node<>(null, e))
                .toList();
        this.shardingType = new ConsistentHash<>(new MD5HashFunction(),ServerConfigConstants.VIRTUAL_NODES, nodes);
    }

    @Override
    public void start() {
        endpointToPort.keySet().forEach(this::start);
    }

    @Override
    public void start(String endpoint) {
        if (endpointToService.containsKey(endpoint)) {
            throw new RuntimeException(endpoint + " is already running");
        }

        try {
            int port = endpointToPort.get(endpoint);
            KVService service = new AndecoShardingKVServiceImpl(
                    port,
                    new FileDao(String.valueOf(port)),
                    shardingType
            );
            service.start();
            endpointToService.put(endpoint, service);
        } catch (IOException e) {
            throw new RuntimeException(endpoint + " is not started", e);
        }
    }

    @Override
    public void stop() {
        Map<String, KVService> safeEndpointToService = new ConcurrentHashMap<>(endpointToService);
        safeEndpointToService.forEach((endpoint, service) -> {
            service.stop();
            endpointToService.remove(endpoint);
        });
    }

    @Override
    public void stop(String endpoint) {
        if (!endpointToService.containsKey(endpoint)) {
            throw new RuntimeException(endpoint + " is not running");
        }

        endpointToService.get(endpoint).stop();
        endpointToService.remove(endpoint);
    }

    @Override
    public List<String> getEndpoints() {
        return new ArrayList<>(endpointToPort.keySet());
    }
}
