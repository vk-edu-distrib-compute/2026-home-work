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
        for (int nodeId : nodeIds) {
            Node node = new Node(nodes, nodeId);
            nodes.put(nodeId, node);
        }
    }

    public void startNodes() {
        for (Node node : nodes.values()) {
            Thread nodeThread = new Thread(node, "node-" + node.getId());
            nodeThread.start();
            node.start();
        }
    }

    public void stopNode(int id) {
        nodes.get(id).stop();
    }

    public void recoverNode(int id) {
        nodes.get(id).recover();
    }

}
