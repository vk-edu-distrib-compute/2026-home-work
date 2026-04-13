package company.vk.edu.distrib.compute.wedwincode.sharded;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.wedwincode.PersistentDao;
import company.vk.edu.distrib.compute.wedwincode.exceptions.ServiceStartException;
import company.vk.edu.distrib.compute.wedwincode.exceptions.ServiceStopException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RendezvousKVClusterImpl implements KVCluster {
    private static final String LOCALHOST_PREFIX = "http://localhost:";
    private static final Path STORAGE_PATH = Path.of(".data", "wedwincode");

    private final Map<String, Integer> endpointToPort;
    private final Map<String, KVService> endpointToService;
    private final HashStrategy strategy;

    public RendezvousKVClusterImpl(List<Integer> ports) {
        this.endpointToPort = buildEndpointsMap(ports);
        this.endpointToService = new ConcurrentHashMap<>();

        List<String> endpoints = new ArrayList<>(endpointToPort.keySet());
        this.strategy = new RendezvousHashStrategy(endpoints);
    }

    @Override
    public void start() {
        endpointToPort.keySet().forEach(this::start);
    }

    @Override
    public void start(String endpoint) {
        if (endpointToService.containsKey(endpoint)) {
            throw new ServiceStartException("service is already running on endpoint " + endpoint);
        }

        try {
            if (!endpointToPort.containsKey(endpoint)) {
                throw new ServiceStartException("endpoint not exist: " + endpoint);
            }
            int port = endpointToPort.get(endpoint);
            KVService service = new ShardedKVServiceImpl(
                    port,
                    buildPersistentDao(port),
                    strategy
            );
            service.start();
            endpointToService.put(endpoint, service);
        } catch (IOException e) {
            throw new ServiceStartException("failed to start endpoint: " + endpoint, e);
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
            throw new ServiceStopException("no services running on endpoint " + endpoint);
        }

        endpointToService.get(endpoint).stop();
        endpointToService.remove(endpoint);
    }

    @Override
    public List<String> getEndpoints() {
        return new ArrayList<>(endpointToPort.keySet());
    }

    private Map<String, Integer> buildEndpointsMap(List<Integer> ports) {
        Map<String, Integer> endpointToPort = new ConcurrentHashMap<>();
        ports.forEach(port -> endpointToPort.put(LOCALHOST_PREFIX + port, port));
        return endpointToPort;
    }

    private Dao<byte[]> buildPersistentDao(int port) throws IOException {
        Path nodePath = STORAGE_PATH.resolve("node" + port);
        return new PersistentDao(nodePath);
    }
}
