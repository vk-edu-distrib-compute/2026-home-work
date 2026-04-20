package company.vk.edu.distrib.compute.ip;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;

public class PopovIgorKVClusterImpl implements KVCluster {
    private final Map<String, KVService> nodes = new ConcurrentHashMap<>();
    private final List<String> endpoints;

    public PopovIgorKVClusterImpl(List<String> endpoints) {
        this.endpoints = endpoints;
        PopovIgorKVServiceFactoryImpl factory = new PopovIgorKVServiceFactoryImpl();

        // set the sharding function
        factory.setRouter(this::getTargetEndpoint);
        
        for (String endpoint : endpoints) {
            try {
                int port = Integer.parseInt(endpoint.substring(endpoint.lastIndexOf(':') + 1));
                nodes.put(endpoint, factory.doCreate(port));
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to create node via factory for " + endpoint, e);
            }
        }
    }

    @Override
    public void start() {
        // стартует все ноды кластера
        endpoints.forEach(this::start);
    }

    @Override
    public void start(String endpoint) {
        // стартует одну определенную ноду кластера
        KVService service = nodes.get(endpoint);
        if (service != null) {
            service.start();
        }
    }

    @Override
    public void stop() {
        // останавливает все ноды кластера
        endpoints.forEach(this::stop);
    }

    @Override
    public void stop(String endpoint) {
        // останавливает одну определенную ноду кластера
        KVService service = nodes.get(endpoint);
        if (service != null) {
            service.stop();
        }
    }

    @Override
    public List<String> getEndpoints() {
        // отдаёт эндпойнты нод кластера
        return new ArrayList<>(endpoints);
    }

    public String getTargetEndpoint(String key) {
        String bestEP = null;
        int maxHash = Integer.MIN_VALUE;

        for (String endpoint : endpoints) {
            int hash = (key + endpoint).hashCode();
            if (hash > maxHash) {
                maxHash = hash;
                bestEP = endpoint;
            }
        }
        return bestEP;
    }
}
