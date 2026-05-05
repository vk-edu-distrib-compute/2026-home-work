package company.vk.edu.distrib.compute.vladislavguzov.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Cluster {

    private static final Logger log = LoggerFactory.getLogger(Cluster.class);

    private final List<Node> nodes;

    public Cluster(int size) {
        nodes = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            nodes.add(new Node(i));
        }
        for (Node node : nodes) {
            node.setPeers(nodes.stream().filter(n -> n != node).toList());
        }
    }

    public void start() {
        for (Node node : nodes) {
            node.start();
        }
    }

    public void stop() {
        for (Node node : nodes) {
            node.shutdown();
        }
        for (Node node : nodes) {
            try {
                node.join(1000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public Node getNode(int id) {
        return nodes.get(id);
    }

    public int getCurrentLeaderId() {
        for (Node node : nodes) {
            if (node.getNodeState() == NodeState.LEADER) {
                return node.getNodeId();
            }
        }
        return -1;
    }

    public int size() {
        return nodes.size();
    }

    public void printStatus() {
        for (Node node : nodes) {
            log.info("Node {}: state={}, leaderId={}",
                node.getNodeId(), node.getNodeState(), node.getLeaderId());
        }
    }
}
