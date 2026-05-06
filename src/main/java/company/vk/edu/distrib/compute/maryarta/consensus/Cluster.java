package company.vk.edu.distrib.compute.maryarta.consensus;

import java.util.*;

public class Cluster {
    public final Map<Integer, Node> nodes;
    private final List<Integer> nodeIds;

    public Cluster(List<Integer> nodeIds) {
        this.nodeIds = nodeIds;
        nodes = new HashMap<>();
        createNodes();
    }

    private void createNodes() {
        nodeIds.forEach(this::addNode);
    }

    private void addNode(int nodeId) {
        nodes.put(nodeId, createNode(nodeId));
    }

    public void startNodes() {
        nodes.values().forEach(this::startNode);
    }

    private void startNode(Node node) {
        Thread nodeThread = createNodeThread(node);
        nodeThread.start();
        node.start();
    }

    private Thread createNodeThread(Node node) {
        return new Thread(node, "node-" + node.getId());
    }

    private Node createNode(int nodeId) {
        return new Node(nodes, nodeId);
    }

    public void stopNode(int id) {
        nodes.get(id).stop();
    }

    public void recoverNode(int id) {
        nodes.get(id).recover();
    }

}
