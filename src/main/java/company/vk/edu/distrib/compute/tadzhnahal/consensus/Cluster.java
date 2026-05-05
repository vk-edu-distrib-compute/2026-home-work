package company.vk.edu.distrib.compute.tadzhnahal.consensus;

import java.lang.System.Logger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Cluster {
    private static final Logger LOG = System.getLogger(Cluster.class.getName());

    private final List<Node> nodes = new ArrayList<>();

    private boolean started;

    public Cluster(int nodeCount) {
        if (nodeCount <= 0) {
            throw new IllegalArgumentException("node count must be positive");
        }

        for (int i = 1; i <= nodeCount; i++) {
            nodes.add(new Node(i));
        }

        for (Node node : nodes) {
            node.setClusterNodes(nodes);
        }
    }

    public List<Node> getNodes() {
        return Collections.unmodifiableList(nodes);
    }

    public synchronized void start() {
        if (started) {
            LOG.log(Logger.Level.INFO, "cluster already started");
            return;
        }

        LOG.log(Logger.Level.INFO, "cluster starts");

        for (Node node : nodes) {
            node.start();
        }

        started = true;
    }

    public synchronized void stop() {
        if (!started) {
            LOG.log(Logger.Level.INFO, "cluster already stopped");
            return;
        }

        LOG.log(Logger.Level.INFO, "cluster stops");

        for (Node node : nodes) {
            node.interrupt();
        }

        waitNodes();

        started = false;

        LOG.log(Logger.Level.INFO, "cluster stopped");
    }

    public void sendMessage(int fromId, int toId, MessageType type) {
        Node sender = findNode(fromId);

        if (sender == null) {
            LOG.log(Logger.Level.WARNING, "cluster cannot find sender node " + fromId);
            return;
        }

        sender.sendMessage(toId, type);
    }

    public void printState() {
        LOG.log(Logger.Level.INFO, "cluster state:");

        for (Node node : nodes) {
            LOG.log(
                    Logger.Level.INFO,
                    "node " + node.getNodeId()
                            + " | " + node.getNodeStatus()
                            + " | threadAlive=" + node.isAlive()
                            + " | inbox=" + node.getInboxSize()
            );
        }
    }

    private Node findNode(int nodeId) {
        for (Node node : nodes) {
            if (node.getNodeId() == nodeId) {
                return node;
            }
        }

        return null;
    }

    private void waitNodes() {
        for (Node node : nodes) {
            try {
                node.join(1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
