package company.vk.edu.distrib.compute.mediocritas.cluster;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.mediocritas.cluster.proxy.HttpProxyClient;
import company.vk.edu.distrib.compute.mediocritas.cluster.routing.Router;
import company.vk.edu.distrib.compute.mediocritas.service.ClusterKvByteService;
import company.vk.edu.distrib.compute.mediocritas.storage.FileByteDao;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class PushkinaKVCluster implements KVCluster {

    private final Map<String, KVService> nodes = new ConcurrentHashMap<>();
    private final List<String> endpoints;

    public PushkinaKVCluster(List<Integer> ports, Router router) {
        HttpProxyClient proxyClient = new HttpProxyClient();

        this.endpoints = ports.stream()
                .map(port -> "http://localhost:" + port)
                .collect(Collectors.toList());

        for (String endpoint : endpoints) {
            router.addNode(endpoint);
        }

        for (int port : ports) {
            try {
                String endpoint = "http://localhost:" + port;
                String dataPath = "./data-cluster-" + port;
                KVService service = new ClusterKvByteService(port, new FileByteDao(dataPath), router, proxyClient);
                nodes.put(endpoint, service);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create cluster node", e);
            }
        }
    }

    @Override
    public void start() {
        for (KVService service : nodes.values()) {
            service.start();
        }
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
        for (KVService service : nodes.values()) {
            service.stop();
        }
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
        return endpoints;
    }
}
