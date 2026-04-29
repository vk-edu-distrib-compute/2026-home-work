package company.vk.edu.distrib.compute.korjick.app.cluster;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CakeKVCluster implements KVCluster, Closeable {
    private final Map<String, KVService> services;

    public CakeKVCluster(Map<String, KVService> services) {
        this.services = Map.copyOf(services);
    }

    @Override
    public void start() {
        services.values().forEach(KVService::start);
    }

    @Override
    public void start(String endpoint) {
        var service = services.get(endpoint);
        if (service != null) {
            service.start();
        }
    }

    @Override
    public void stop() {
        services.values().forEach(KVService::stop);
    }

    @Override
    public void stop(String endpoint) {
        KVService service = services.get(endpoint);
        if (service != null) {
            service.stop();
        }
    }

    @Override
    public void close() throws IOException {
        for (KVService service : services.values()) {
            if (service instanceof Closeable closeable) {
                closeable.close();
            }
        }
    }

    @Override
    public List<String> getEndpoints() {
        return this.services.keySet().stream().toList();
    }
}
