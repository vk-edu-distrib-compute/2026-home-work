package company.vk.edu.distrib.compute.che1nov.consensus;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ClusterModel {
    private final Map<Integer, ClusterNode> nodes = new ConcurrentHashMap<>();

    public ClusterModel(
            List<Integer> nodeIds,
            Duration pingInterval,
            Duration pingTimeout,
            Duration electionTimeout,
            double failProbability,
            Duration minRecover,
            Duration maxRecover
    ) {
        for (Integer id : nodeIds) {
            nodes.put(
                    id,
                    new ClusterNode(
                            id,
                            pingInterval,
                            pingTimeout,
                            electionTimeout,
                            failProbability,
                            minRecover,
                            maxRecover
                    )
            );
        }
        for (ClusterNode node : nodes.values()) {
            node.setPeers(nodes);
        }
    }

    public void start() {
        for (ClusterNode node : nodes.values()) {
            node.start();
        }
    }

    public void stop() {
        for (ClusterNode node : nodes.values()) {
            node.shutdownNode();
        }
        for (ClusterNode node : nodes.values()) {
            try {
                node.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void forceDown(int nodeId) {
        node(nodeId).forceDown();
    }

    public void forceUp(int nodeId) {
        node(nodeId).forceUp();
    }

    public List<ClusterNode> liveNodes() {
        return nodes.values().stream().filter(ClusterNode::isAliveNode).collect(Collectors.toList());
    }

    public Integer maxLiveNodeId() {
        return liveNodes().stream().map(ClusterNode::nodeId).max(Integer::compareTo).orElse(null);
    }

    public List<ClusterNode> leaders() {
        List<ClusterNode> result = new ArrayList<>();
        for (ClusterNode node : nodes.values()) {
            if (node.isAliveNode() && node.role() == NodeRole.LEADER) {
                result.add(node);
            }
        }
        return result;
    }

    public ClusterNode node(int nodeId) {
        ClusterNode node = nodes.get(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("Unknown node id: " + nodeId);
        }
        return node;
    }

    public String snapshot() {
        return nodes.values().stream()
                .sorted((a, b) -> Integer.compare(a.nodeId(), b.nodeId()))
                .map(node -> String.format(
                        "{id=%d, role=%s, alive=%s, leaderId=%s}",
                        node.nodeId(),
                        node.role(),
                        node.isAliveNode(),
                        node.leaderId()
                ))
                .collect(Collectors.joining(" "));
    }
}
