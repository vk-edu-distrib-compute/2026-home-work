package company.vk.edu.distrib.compute.andeco.sharding;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.andeco.ServerConfigConstants;
import company.vk.edu.distrib.compute.andeco.consistent_hash.MD5HashFunction;
import company.vk.edu.distrib.compute.andeco.rendezvous_hash.RendezvousHash;
import company.vk.edu.distrib.compute.andeco.replica.AndecoReplicatedService;

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
        this.shardingType = new RendezvousHash<>(new MD5HashFunction(), nodes);
    }

    @Override
    public void start() {
        endpointToPort.keySet().forEach(this::start);
    }

    @Override
    public void start(String endpoint) {
        if (endpointToService.containsKey(endpoint)) {
            throw new IllegalStateException(endpoint + " is already running");
        }

        try {
            int port = endpointToPort.get(endpoint);

            KVService service = new AndecoReplicatedService(
                    port,
                    shardingType,
                        3
            );

            service.start();
            endpointToService.put(endpoint, service);
        } catch (IOException e) {
            throw new IllegalStateException(endpoint + " failed to start", e);
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
        KVService service = endpointToService.get(endpoint);

        if (service == null) {
            throw new IllegalStateException(endpoint + " is not running");
        }

        service.stop();
        endpointToService.remove(endpoint);
    }

    @Override
    public List<String> getEndpoints() {
        return new ArrayList<>(endpointToPort.keySet());
    }
}
