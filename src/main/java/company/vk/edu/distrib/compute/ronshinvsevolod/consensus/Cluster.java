package company.vk.edu.distrib.compute.ronshinvsevolod.consensus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.Map;

public final class Cluster {
    private final List<Node> nodes;

    public Cluster(final int size) {
        this.nodes = new ArrayList<>(size);
        for (int id = 1; id <= size; id++) {
            nodes.add(new Node(id));
        }
        for (final Node node : nodes) {
            final List<Node> peers = nodes.stream()
                .filter(nd -> nd.getNodeId() != node.getNodeId())
                .toList();
            node.setPeers(Collections.unmodifiableList(peers));
        }
    }

    public void start() {
        for (final Node node : nodes) {
            node.start();
        }
    }

    public void stop() {
        for (final Node node : nodes) {
            node.stop();
        }
    }

    public void failNode(final int nodeId) {
        findNode(nodeId).fail();
    }

    public void recoverNode(final int nodeId) {
        findNode(nodeId).recover();
    }

    public int getLeaderId() {
        return nodes.stream()
            .filter(nd -> nd.getState() != NodeState.FAILED)
            .mapToInt(Node::getLeaderId)
            .filter(id -> id > 0)
            .boxed()
            .collect(Collectors.groupingBy(id -> id, Collectors.counting()))
            .entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .orElse(-1);
    }

    public Node getNode(final int nodeId) {
        return findNode(nodeId);
    }

    public int getMaxAliveId() {
        final OptionalInt max = nodes.stream()
            .filter(nd -> nd.getState() != NodeState.FAILED)
            .mapToInt(Node::getNodeId)
            .max();
        return max.orElse(-1);
    }

    private Node findNode(final int nodeId) {
        return nodes.stream()
            .filter(nd -> nd.getNodeId() == nodeId)
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Node not found: " + nodeId));
    }
}

