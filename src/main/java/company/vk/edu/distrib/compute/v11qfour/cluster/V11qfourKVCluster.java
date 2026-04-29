package company.vk.edu.distrib.compute.v11qfour.cluster;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class V11qfourKVCluster implements KVCluster {
    private final Map<String, KVService> nodes;

    public V11qfourKVCluster(Map<String, KVService> nodes) {
        this.nodes = nodes;
    }

    @Override
    public void start() {
        nodes.values().forEach(KVService::start);
    }

    @Override
    public void start(String endpoint) {
        KVService service = nodes.get(endpoint);
        if (service != null) {
            service.start();
        }
    }

    @Override
    public void stop() {
        nodes.values().forEach(KVService::stop);
    }

    @Override
    public void stop(String endpoint) {
        KVService service = nodes.get(endpoint);
        if (service != null) {
            service.stop();
        }
    }

    @Override
    public List<String> getEndpoints() {
        return new ArrayList<>(nodes.keySet());
    }
}
