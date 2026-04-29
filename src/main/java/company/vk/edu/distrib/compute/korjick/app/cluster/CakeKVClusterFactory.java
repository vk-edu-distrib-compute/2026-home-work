package company.vk.edu.distrib.compute.korjick.app.cluster;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.korjick.adapters.input.grpc.CakeGrpcServer;
import company.vk.edu.distrib.compute.korjick.adapters.input.http.CakeHttpServer;
import company.vk.edu.distrib.compute.korjick.adapters.input.http.entity.EntityHandler;
import company.vk.edu.distrib.compute.korjick.adapters.input.http.status.StatusHandler;
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
                final var endpoint = CakeGrpcServer.resolveEndpoint(host, port);
                final var repository = new H2EntityRepository(String.format("node_%s", port));
                final var coordinator = new ShardingKVCoordinator(repository, endpoint, endpoints, gateway);

                final var httpServer = new CakeHttpServer(host, port);
                httpServer.register("/v0/status", new StatusHandler());
                httpServer.register("/v0/entity", new EntityHandler(coordinator));
                final var grpcServer = new CakeGrpcServer(host, port, coordinator);
                final var service = new CakeKVClusterNodeService(repository, httpServer, grpcServer);
                nodes.put(endpoint, service);
            }

            return new CakeKVCluster(nodes);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create cluster", e);
        }
    }
}
