package company.vk.edu.distrib.compute.nihuaway00;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.nihuaway00.sharding.NodeInfo;

import java.io.IOException;
import java.util.*;

public class NihuawayKVCluster implements KVCluster {
    private final List<NodeInfo> nodes;
    private final Map<String, KVService> services = new HashMap<>();
    private final KVServiceFactory serviceFactory;

    public NihuawayKVCluster(List<NodeInfo> nodes, KVServiceFactory serviceFactory) {
        this.nodes = nodes;
        this.serviceFactory = serviceFactory;
    }

    @Override
    public void start() {
        nodes.forEach(n -> {
            try {
                KVService service = serviceFactory.create(n.getPort());
                service.start();
                services.put(n.getEndpoint(), service);
                n.setAlive(true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void start(String endpoint) {
        String[] parts = endpoint.split(":");
        int port = Integer.parseInt(parts[parts.length - 1]);


        Optional<NodeInfo> candidateNode = nodes.stream().filter(n -> n.getPort() == port).findFirst();

        if (candidateNode.isEmpty()) {
            throw new IllegalArgumentException("node with port " + port + " not found");
        }

        NodeInfo node = candidateNode.get();

        try {
            KVService service = serviceFactory.create(port);
            service.start();
            services.put(node.getEndpoint(), service);
            node.setAlive(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        nodes.forEach(n -> {
            KVService service = services.get(n.getEndpoint());
            service.stop();
            n.setAlive(false);
        });
    }

    @Override
    public void stop(String endpoint) {
        String[] parts = endpoint.split(":");
        int port = Integer.parseInt(parts[parts.length - 1]);
        nodes.stream().filter(n -> n.getPort() == port).findFirst().ifPresent(n -> {
            services.get(n.getEndpoint()).stop();
            n.setAlive(false);
        });
    }

    @Override
    public List<String> getEndpoints() {
        return nodes.stream().map(NodeInfo::getEndpoint).toList();
    }
}