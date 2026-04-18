package company.vk.edu.distrib.compute.dariaprindina;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DPKvCluster implements KVCluster {
    private static final Logger log = LoggerFactory.getLogger(DPKvCluster.class);

    private final List<String> endpoints;
    private final Map<String, NodeState> nodesByEndpoint;

    public DPKvCluster(List<Integer> ports) {
        this.endpoints = ports.stream()
            .map(port -> "http://localhost:" + port)
            .toList();
        this.nodesByEndpoint = new LinkedHashMap<>();
        for (String endpoint : endpoints) {
            this.nodesByEndpoint.put(endpoint, new NodeState(endpoint, new DPDao()));
        }
    }

    @Override
    public void start() {
        for (NodeState nodeState : nodesByEndpoint.values()) {
            startNode(nodeState);
        }
        log.info("Cluster started. endpoints={}", endpoints);
    }

    @Override
    public void start(String endpoint) {
        startNode(findNode(endpoint));
    }

    @Override
    public void stop() {
        for (NodeState nodeState : nodesByEndpoint.values()) {
            stopNode(nodeState);
        }
        log.info("Cluster stopped. endpoints={}", endpoints);
    }

    @Override
    public void stop(String endpoint) {
        stopNode(findNode(endpoint));
    }

    @Override
    public List<String> getEndpoints() {
        return endpoints;
    }

    private void startNode(NodeState nodeState) {
        if (nodeState.started) {
            return;
        }
        try {
            nodeState.service = new DPShardedNodeService(nodeState.endpoint, endpoints, nodeState.dao);
            nodeState.service.start();
            nodeState.started = true;
            log.info("Node started. endpoint={}", nodeState.endpoint);
        } catch (IOException e) {
            throw new UncheckedIOException("Can't start node " + nodeState.endpoint, e);
        }
    }

    private void stopNode(NodeState nodeState) {
        if (!nodeState.started || nodeState.service == null) {
            return;
        }
        nodeState.service.stop();
        nodeState.service = null;
        nodeState.started = false;
        log.info("Node stopped. endpoint={}", nodeState.endpoint);
    }

    private NodeState findNode(String endpoint) {
        final NodeState nodeState = nodesByEndpoint.get(endpoint);
        if (nodeState == null) {
            throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
        }
        return nodeState;
    }

    private static final class NodeState {
        private final String endpoint;
        private final Dao<byte[]> dao;
        private DPShardedNodeService service;
        private boolean started;

        private NodeState(String endpoint, Dao<byte[]> dao) {
            this.endpoint = endpoint;
            this.dao = dao;
        }
    }
}
