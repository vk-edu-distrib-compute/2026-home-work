package company.vk.edu.distrib.compute.mariguss;

import company.vk.edu.distrib.compute.KVCluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MarigussKVCluster implements KVCluster {
    private final List<String> endpoints;
    private final Map<String, MarigussKVService> services;

    public MarigussKVCluster(List<Integer> ports) {
        this.endpoints = new ArrayList<>();
        this.services = new HashMap<>();

        for (int port : ports) {
            this.endpoints.add("http://localhost:" + port);
        }

        for (int port : ports) {
            try {
                MarigussKVService service = createService(port);
                service.setClusterEndpoints(this.endpoints);
                this.services.put("http://localhost:" + port, service);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to create service", e);
            }
        }
    }

    @Override
    public void start() {
        services.values().forEach(MarigussKVService::start);
    }

    @Override
    public void start(String endpoint) {
        if (services.containsKey(endpoint)) {
            services.get(endpoint).start();
        }
    }

    @Override
    public void stop() {
        services.values().forEach(MarigussKVService::stop);
    }

    @Override
    public void stop(String endpoint) {
        if (services.containsKey(endpoint)) {
            services.get(endpoint).stop();
        }
    }

    @Override
    public List<String> getEndpoints() {
        return new ArrayList<>(endpoints);
    }

    private MarigussKVService createService(int port) throws IOException {
        return new MarigussKVService(port);
    }
}
