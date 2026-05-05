package company.vk.edu.distrib.compute.korjick.app.cluster;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.korjick.adapters.input.grpc.CakeGrpcServer;
import company.vk.edu.distrib.compute.korjick.adapters.output.GrpcEntityGateway;
import company.vk.edu.distrib.compute.korjick.adapters.output.H2EntityRepository;
import company.vk.edu.distrib.compute.korjick.app.node.CakeKVNode;
import company.vk.edu.distrib.compute.korjick.core.application.coordinator.DistributedKVCoordinator;
import company.vk.edu.distrib.compute.korjick.core.application.node.EntityNode;
import company.vk.edu.distrib.compute.korjick.core.application.node.LocalEntityNode;
import company.vk.edu.distrib.compute.korjick.core.application.node.RemoteEntityNode;
import company.vk.edu.distrib.compute.korjick.ports.output.EntityRepository;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CakeKVClusterFactory extends KVClusterFactory {
    private static final int SHARD_ONLY_REPLICATION_FACTOR = 1;

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        final var gateway = new GrpcEntityGateway();
        final var host = "localhost";
        final Map<String, CakeKVNode> nodes = new ConcurrentHashMap<>();
        final List<String> endpoints = ports.stream()
                .map(port -> CakeGrpcServer.resolveEndpoint(host, port))
                .toList();

        try {
            for (int port : ports) {
                final var grpcEndpoint = CakeGrpcServer.resolveEndpoint(host, port);
                final var repository = new H2EntityRepository(String.format("node_%s", port));
                final var coordinator = new DistributedKVCoordinator(
                        nodes(grpcEndpoint, endpoints, repository, gateway),
                        SHARD_ONLY_REPLICATION_FACTOR
                );
                final var node = new CakeKVNode(repository, host, port, coordinator);
                nodes.put(node.httpEndpoint(), node);
            }

            return new CakeKVCluster(nodes);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create cluster", e);
        }
    }

    private List<EntityNode> nodes(String currentEndpoint,
                                   List<String> endpoints,
                                   EntityRepository repository,
                                   GrpcEntityGateway gateway) {
        return endpoints.stream()
                .map(endpoint -> node(currentEndpoint, endpoint, repository, gateway))
                .toList();
    }

    private EntityNode node(String currentEndpoint,
                            String endpoint,
                            EntityRepository repository,
                            GrpcEntityGateway gateway) {
        if (endpoint.equals(currentEndpoint)) {
            return new LocalEntityNode(endpoint, repository);
        }
        return new RemoteEntityNode(endpoint, gateway);
    }
}
