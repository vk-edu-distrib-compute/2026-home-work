package company.vk.edu.distrib.compute.korjick.app.replicated;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.korjick.adapters.output.GrpcEntityGateway;
import company.vk.edu.distrib.compute.korjick.adapters.output.H2EntityRepository;
import company.vk.edu.distrib.compute.korjick.core.application.ReplicatedKVCoordinator;
import company.vk.edu.distrib.compute.korjick.core.application.SingleNodeCoordinator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CakeReplicatedKVServiceFactory extends KVServiceFactory {
    private static final String HOST = "localhost";
    private static final int DEFAULT_REPLICATION_FACTOR = 5;
    private static final int REPLICA_PORT_SHIFT = 1_000;

    @Override
    protected KVService doCreate(int port) throws IOException {
        List<ReplicatedCakeKVService.Node> nodes = new ArrayList<>(DEFAULT_REPLICATION_FACTOR);

        for (int replicaNumber = 0; replicaNumber < DEFAULT_REPLICATION_FACTOR; replicaNumber++) {
            var repository = new H2EntityRepository(String.format("node_%s_replica_%s", port, replicaNumber));

            var localCoordinator = new SingleNodeCoordinator(repository);
            int replicaPort = replicaPort(port, replicaNumber);
            nodes.add(new ReplicatedCakeKVService.Node(repository, HOST, replicaPort, localCoordinator));
        }

        var replicatedCoordinator = new ReplicatedKVCoordinator(
                nodes.stream()
                        .map(ReplicatedCakeKVService.Node::endpoint)
                        .toList(),
                new GrpcEntityGateway(),
                endpoint -> nodes.stream()
                        .filter(node -> node.endpoint().equals(endpoint))
                        .findFirst()
                        .map(ReplicatedCakeKVService.Node::isStarted)
                        .orElse(false),
                nodes.size()
        );
        return new ReplicatedCakeKVService(HOST, port, nodes, replicatedCoordinator);
    }

    private int replicaPort(int servicePort, int replicaNumber) {
        return servicePort + REPLICA_PORT_SHIFT + replicaNumber;
    }
}
