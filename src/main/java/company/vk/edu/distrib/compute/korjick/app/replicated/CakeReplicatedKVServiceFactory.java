package company.vk.edu.distrib.compute.korjick.app.replicated;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.korjick.adapters.output.H2EntityRepository;
import company.vk.edu.distrib.compute.korjick.app.node.CakeKVNode;
import company.vk.edu.distrib.compute.korjick.core.application.coordinator.DistributedKVCoordinator;
import company.vk.edu.distrib.compute.korjick.core.application.node.EntityNode;
import company.vk.edu.distrib.compute.korjick.core.application.node.LocalEntityNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CakeReplicatedKVServiceFactory extends KVServiceFactory {
    private static final String HOST = "localhost";
    private static final int DEFAULT_REPLICATION_FACTOR = 5;
    private static final int REPLICA_PORT_SHIFT = 1_000;

    @Override
    protected KVService doCreate(int port) throws IOException {
        List<CakeKVNode> nodes = new ArrayList<>(DEFAULT_REPLICATION_FACTOR);
        List<EntityNode> replicaNodes = new ArrayList<>(DEFAULT_REPLICATION_FACTOR);

        for (int replicaNumber = 0; replicaNumber < DEFAULT_REPLICATION_FACTOR; replicaNumber++) {
            var repository = new H2EntityRepository(String.format("node_%s_replica_%s", port, replicaNumber));

            int replicaPort = replicaPort(port, replicaNumber);
            var node = new CakeKVNode(repository, HOST, replicaPort);
            nodes.add(node);
            replicaNodes.add(new LocalEntityNode(node.grpcEndpoint(), repository));
        }

        var distributedCoordinator = new DistributedKVCoordinator(
                replicaNodes,
                nodes.size()
        );
        return new ReplicatedCakeKVService(HOST, port, nodes, distributedCoordinator);
    }

    private int replicaPort(int servicePort, int replicaNumber) {
        return servicePort + REPLICA_PORT_SHIFT + replicaNumber;
    }
}
