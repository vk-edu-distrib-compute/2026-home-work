package company.vk.edu.distrib.compute.nst1610.consensus;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class ConsensusCluster implements AutoCloseable {
    private final ConsensusConfig config;
    private final Map<Integer, Node> nodes;

    public ConsensusCluster(Set<Integer> nodeIds, ConsensusConfig config) {
        this.config = config;
        Map<Integer, Node> nodes = new LinkedHashMap<>();
        for (int nodeId : nodeIds.stream().sorted().toList()) {
            ElectionCoordinator electionCoordinator = new ElectionCoordinator();
            nodes.put(nodeId, new Node(nodeId, config, electionCoordinator));
        }
        this.nodes = Map.copyOf(nodes);
        for (Node node : nodes.values()) {
            node.setCluster(nodes);
        }
    }

    public static ConsensusCluster withNodeCount(int nodeCount, ConsensusConfig config) {
        Set<Integer> ids = IntStream.rangeClosed(1, nodeCount)
            .boxed()
            .collect(Collectors.toCollection(LinkedHashSet::new));
        return new ConsensusCluster(ids, config);
    }

    public void start() {
        for (Node node : nodes.values()) {
            node.start();
        }
    }

    public void stop() {
        for (Node node : nodes.values()) {
            node.shutdownNode();
        }
        for (Node node : nodes.values()) {
            try {
                node.join(2_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while stopping cluster", e);
            }
        }
    }

    public void failNode(int nodeId) {
        getNode(nodeId).failNode();
    }

    public void recoverNode(int nodeId) {
        getNode(nodeId).recoverNode();
    }

    public Node getNode(int nodeId) {
        Node node = nodes.get(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("Unknown node: " + nodeId);
        }
        return node;
    }

    public List<NodeSnapshot> snapshots() {
        List<NodeSnapshot> snapshots = new ArrayList<>();
        for (Node node : nodes.values()) {
            snapshots.add(node.snapshot());
        }
        return Node.sortSnapshots(snapshots);
    }

    public int getHighestAliveNodeId() {
        return snapshots().stream()
            .filter(snapshot -> !snapshot.failed())
            .mapToInt(NodeSnapshot::nodeId)
            .max()
            .orElse(-1);
    }

    @Override
    public void close() {
        stop();
    }
}
