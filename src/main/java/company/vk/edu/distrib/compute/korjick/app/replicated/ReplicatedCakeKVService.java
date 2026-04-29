package company.vk.edu.distrib.compute.korjick.app.replicated;

import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.korjick.adapters.input.grpc.CakeGrpcServer;
import company.vk.edu.distrib.compute.korjick.adapters.input.http.CakeHttpServer;
import company.vk.edu.distrib.compute.korjick.ports.output.EntityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReplicatedCakeKVService implements ReplicatedService {
    private static final Logger log = LoggerFactory.getLogger(ReplicatedCakeKVService.class);

    private final int port;
    private final List<Node> nodes;
    private final CakeHttpServer httpServer;

    public ReplicatedCakeKVService(int port,
                                   List<Node> nodes,
                                   CakeHttpServer httpServer) {
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("Replication factor should be positive");
        }

        this.port = port;
        this.nodes = List.copyOf(nodes);
        this.httpServer = httpServer;
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
        node(nodeId).disable();
    }

    @Override
    public void enableReplica(int nodeId) {
        node(nodeId).enable();
    }

    @Override
    public void start() {
        nodes.forEach(Node::start);
        httpServer.start();
        log.info("ReplicatedCakeKVService started on port={} with replicationFactor={}", port, nodes.size());
    }

    @Override
    public void stop() {
        httpServer.stop();
        nodes.forEach(Node::stop);
        for (int nodeId = 0; nodeId < nodes.size(); nodeId++) {
            try {
                nodes.get(nodeId).close();
            } catch (IOException e) {
                log.error("Failed to close replica repository id={}", nodeId, e);
            }
        }
        log.info("ReplicatedCakeKVService stopped");
    }

    public List<String> replicaEndpoints() {
        return nodes.stream()
                .map(Node::endpoint)
                .toList();
    }

    public boolean endpointAvailable(String endpoint) {
        return nodes.stream()
                .filter(node -> Objects.equals(node.endpoint(), endpoint))
                .findFirst()
                .map(Node::isEnabled)
                .orElse(false);
    }

    private Node node(int nodeId) {
        if (nodeId < 0 || nodeId >= nodes.size()) {
            throw new IllegalArgumentException("Replica node id is out of range: " + nodeId);
        }
        return nodes.get(nodeId);
    }

    public static final class Node {
        private final EntityRepository repository;
        private final CakeGrpcServer grpcServer;
        private final AtomicBoolean enabled = new AtomicBoolean(true);

        Node(EntityRepository repository, CakeGrpcServer grpcServer) {
            this.repository = Objects.requireNonNull(repository, "repository");
            this.grpcServer = Objects.requireNonNull(grpcServer, "grpcServer");
        }

        String endpoint() {
            return grpcServer.getEndpoint();
        }

        boolean isEnabled() {
            return enabled.get();
        }

        void disable() {
            enabled.set(false);
        }

        void enable() {
            enabled.set(true);
        }

        void start() {
            grpcServer.start();
        }

        void stop() {
            grpcServer.stop();
        }

        void close() throws IOException {
            repository.close();
        }
    }
}
