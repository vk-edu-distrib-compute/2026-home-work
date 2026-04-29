package company.vk.edu.distrib.compute.korjick.app.cluster;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.korjick.adapters.input.grpc.CakeGrpcServer;
import company.vk.edu.distrib.compute.korjick.adapters.output.GrpcEntityGateway;
import company.vk.edu.distrib.compute.korjick.adapters.output.H2EntityRepository;
import company.vk.edu.distrib.compute.korjick.core.application.ShardingKVCoordinator;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CakeKVClusterFactory extends KVClusterFactory {
    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        final var gateway = new GrpcEntityGateway();
        final var host = "localhost";
        final Map<String, KVService> nodes = new ConcurrentHashMap<>();
        final List<String> endpoints = ports.stream()
                .map(port -> CakeGrpcServer.resolveEndpoint(host, port))
                .toList();

        try {
            for (int port : ports) {
                final var grpcEndpoint = CakeGrpcServer.resolveEndpoint(host, port);
                final var repository = new H2EntityRepository(String.format("node_%s", port));
                final var coordinator = new ShardingKVCoordinator(repository, grpcEndpoint, endpoints, gateway);
                final var service = new CakeKVClusterNodeService(repository, host, port, coordinator);
                nodes.put(String.format("http://%s:%s", host, port), service);
            }

            return new CakeKVCluster(nodes);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create cluster", e);
        }
    }
}
