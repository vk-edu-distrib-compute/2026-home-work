package company.vk.edu.distrib.compute.korjick.cluster;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;

import java.util.List;
import java.util.Map;

public class CakeKVCluster implements KVCluster {

    private final Map<String, KVService> serviceMap;

    public CakeKVCluster(Map<String, KVService> services) {
        this.serviceMap = services;
    }

    @Override
    public void start() {
        this.serviceMap.values().forEach(KVService::start);
    }

    @Override
    public void start(String endpoint) {
        this.serviceMap.get(endpoint).start();
    }

    @Override
    public void stop() {
        this.serviceMap.values().forEach(KVService::stop);
    }

    @Override
    public void stop(String endpoint) {
        this.serviceMap.get(endpoint).stop();
    }

    @Override
    public List<String> getEndpoints() {
        return this.serviceMap.keySet().stream().toList();
    }
}
