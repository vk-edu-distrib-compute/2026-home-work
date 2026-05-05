package company.vk.edu.distrib.compute.korjick.app.replicated;

import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.korjick.adapters.input.http.CakeHttpServer;
import company.vk.edu.distrib.compute.korjick.adapters.input.http.entity.DistributedEntityHandler;
import company.vk.edu.distrib.compute.korjick.adapters.input.http.status.StatusHandler;
import company.vk.edu.distrib.compute.korjick.app.node.CakeKVNode;
import company.vk.edu.distrib.compute.korjick.core.application.coordinator.DistributedKVCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public class ReplicatedCakeKVService implements ReplicatedService, Closeable {
    private static final Logger log = LoggerFactory.getLogger(ReplicatedCakeKVService.class);

    private final String host;
    private final int port;
    private final List<CakeKVNode> nodes;
    private final DistributedKVCoordinator coordinator;
    private CakeHttpServer httpServer;

    public ReplicatedCakeKVService(String host,
                                   int port,
                                   List<CakeKVNode> nodes,
                                   DistributedKVCoordinator coordinator) {
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("Replication factor should be positive");
        }

        this.host = host;
        this.port = port;
        this.nodes = List.copyOf(nodes);
        this.coordinator = coordinator;
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
        node(nodeId).stopGrpc();
    }

    @Override
    public void enableReplica(int nodeId) {
        node(nodeId).startGrpc();
    }

    @Override
    public void start() {
        nodes.forEach(CakeKVNode::startGrpc);
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
        nodes.forEach(CakeKVNode::stopGrpc);
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

    private CakeKVNode node(int nodeId) {
        if (nodeId < 0 || nodeId >= nodes.size()) {
            throw new IllegalArgumentException("Replica node id is out of range: " + nodeId);
        }
        return nodes.get(nodeId);
    }
}
