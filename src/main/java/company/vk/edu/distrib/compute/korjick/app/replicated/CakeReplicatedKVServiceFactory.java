package company.vk.edu.distrib.compute.korjick.app.replicated;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.korjick.adapters.input.grpc.CakeGrpcServer;
import company.vk.edu.distrib.compute.korjick.adapters.input.http.CakeHttpServer;
import company.vk.edu.distrib.compute.korjick.adapters.input.http.entity.DistributedEntityHandler;
import company.vk.edu.distrib.compute.korjick.adapters.input.http.status.StatusHandler;
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
            var grpcServer = new CakeGrpcServer(HOST, replicaPort, localCoordinator);
            nodes.add(new ReplicatedCakeKVService.Node(repository, grpcServer));
        }

        CakeHttpServer httpServer = new CakeHttpServer(HOST, port);
        var service = new ReplicatedCakeKVService(port, nodes, httpServer);
        var replicatedCoordinator = new ReplicatedKVCoordinator(
                service.replicaEndpoints(),
                new GrpcEntityGateway(),
                service::endpointAvailable,
                service.numberOfReplicas()
        );
        httpServer.register("/v0/status", new StatusHandler());
        httpServer.register("/v0/entity", new DistributedEntityHandler(replicatedCoordinator));

        return service;
    }

    private int replicaPort(int servicePort, int replicaNumber) {
        return servicePort + REPLICA_PORT_SHIFT + replicaNumber;
    }
}
