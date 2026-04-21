package company.vk.edu.distrib.compute.solntseva_nastya;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import java.util.List;

public class SolntsevaKVCluster implements KVCluster {
    private final List<KVService> services;
    private final List<String> endpoints;

    public SolntsevaKVCluster(List<KVService> services, List<String> endpoints) {
        this.services = services;
        this.endpoints = endpoints;
    }

    @Override
    public List<String> getEndpoints() {
        return endpoints;
    }

    @Override
    public void start() {
        services.forEach(KVService::start);
    }

    @Override
    public void start(String endpoint) {
        int idx = endpoints.indexOf(endpoint);
        if (idx >= 0) {
            services.get(idx).start();
        }
    }

    @Override
    public void stop() {
        services.forEach(KVService::stop);
    }

    @Override
    public void stop(String endpoint) {
        int idx = endpoints.indexOf(endpoint);
        if (idx >= 0) {
            services.get(idx).stop();
        }
    }
}
