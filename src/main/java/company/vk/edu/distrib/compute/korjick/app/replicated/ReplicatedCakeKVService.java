package company.vk.edu.distrib.compute.korjick.app.replicated;

import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.korjick.adapters.input.grpc.CakeGrpcServer;
import company.vk.edu.distrib.compute.korjick.adapters.input.http.CakeHttpServer;
import company.vk.edu.distrib.compute.korjick.adapters.input.http.entity.DistributedEntityHandler;
import company.vk.edu.distrib.compute.korjick.adapters.input.http.status.StatusHandler;
import company.vk.edu.distrib.compute.korjick.core.application.ReplicatedKVCoordinator;
import company.vk.edu.distrib.compute.korjick.core.application.SingleNodeCoordinator;
import company.vk.edu.distrib.compute.korjick.ports.output.EntityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class ReplicatedCakeKVService implements ReplicatedService, Closeable {
    private static final Logger log = LoggerFactory.getLogger(ReplicatedCakeKVService.class);

    private final String host;
    private final int port;
    private final List<Node> nodes;
    private final ReplicatedKVCoordinator coordinator;
    private CakeHttpServer httpServer;

    public ReplicatedCakeKVService(String host,
                                   int port,
                                   List<Node> nodes,
                                   ReplicatedKVCoordinator coordinator) {
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("Replication factor should be positive");
        }

        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
        this.nodes = List.copyOf(Objects.requireNonNull(nodes, "nodes"));
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public int numberOfReplicas() {
        return nodes.size();
    }

    @Override
    public void disableReplica(int nodeId) {
        node(nodeId).stop();
    }

    @Override
    public void enableReplica(int nodeId) {
        node(nodeId).start();
    }

    @Override
    public void start() {
        nodes.forEach(Node::start);
        try {
            httpServer = new CakeHttpServer(host, port);
            httpServer.register("/v0/status", new StatusHandler());
            httpServer.register("/v0/entity", new DistributedEntityHandler(coordinator));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create replicated HTTP server", e);
        }
        httpServer.start();
        log.info("ReplicatedCakeKVService started on port={} with replicationFactor={}", port, nodes.size());
    }

    @Override
    public void stop() {
        if (httpServer != null) {
            httpServer.stop();
            httpServer = null;
        }
        nodes.forEach(Node::stop);
        log.info("ReplicatedCakeKVService stopped");
    }

    @Override
    public void close() {
        stop();
        for (int nodeId = 0; nodeId < nodes.size(); nodeId++) {
            try {
                nodes.get(nodeId).close();
            } catch (IOException e) {
                log.error("Failed to close replica repository id={}", nodeId, e);
            }
        }
    }

    private Node node(int nodeId) {
        if (nodeId < 0 || nodeId >= nodes.size()) {
            throw new IllegalArgumentException("Replica node id is out of range: " + nodeId);
        }
        return nodes.get(nodeId);
    }

    public static final class Node {
        private final EntityRepository repository;
        private final String host;
        private final int port;
        private final SingleNodeCoordinator coordinator;
        private final String endpoint;
        private CakeGrpcServer grpcServer;

        public Node(EntityRepository repository,
             String host,
             int port,
             SingleNodeCoordinator coordinator) {
            this.repository = Objects.requireNonNull(repository, "repository");
            this.host = Objects.requireNonNull(host, "host");
            this.port = port;
            this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
            this.endpoint = CakeGrpcServer.resolveEndpoint(host, port);
        }

        public String endpoint() {
            return endpoint;
        }

        public boolean isStarted() {
            return grpcServer != null;
        }

        public void start() {
            if (grpcServer != null) {
                return;
            }
            grpcServer = new CakeGrpcServer(host, port, coordinator);
            grpcServer.start();
        }

        public void stop() {
            if (grpcServer != null) {
                grpcServer.stop();
                grpcServer = null;
            }
        }

        public void close() throws IOException {
            stop();
            repository.close();
        }
    }
}
