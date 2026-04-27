package company.vk.edu.distrib.compute.ronshinvsevolod;

import company.vk.edu.distrib.compute.KVCluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GrpcKVCluster implements KVCluster {

    private final Map<String, GrpcKVService> services = new ConcurrentHashMap<>();
    private final List<String> endpoints;

    public GrpcKVCluster(List<NodeConfig> configs) {
        this.endpoints = new ArrayList<>();
        for (NodeConfig cfg : configs) {
            endpoints.add("localhost:" + cfg.httpPort() + "?grpcPort=" + cfg.grpcPort());
        }
        for (int i = 0; i < configs.size(); i++) {
            NodeConfig cfg = configs.get(i);
            List<String> peers = new ArrayList<>();
            for (int j = 0; j < configs.size(); j++) {
                if (j != i) {
                    peers.add("localhost:" + configs.get(j).grpcPort());
                }
            }
            GrpcKVService svc = new GrpcKVService(
                    cfg.httpPort(),
                    cfg.grpcPort(),
                    new InMemoryDao(),
                    peers);
            services.put(endpoints.get(i), svc);
        }
    }

    @Override
    public void start() {
        services.values().forEach(GrpcKVService::start);
    }

    @Override
    public void start(String endpoint) {
        GrpcKVService svc = services.get(endpoint);
        if (svc != null) {
            svc.start();
        }
    }

    @Override
    public void stop() {
        services.values().forEach(GrpcKVService::stop);
    }

    @Override
    public void stop(String endpoint) {
        GrpcKVService svc = services.get(endpoint);
        if (svc != null) {
            svc.stop();
        }
    }

    @Override
    public List<String> getEndpoints() {
        return List.copyOf(endpoints);
    }

    public record NodeConfig(int httpPort, int grpcPort) {
    }
}
