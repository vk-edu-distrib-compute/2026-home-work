package company.vk.edu.distrib.compute.tadzhnahal.consensus;

import java.lang.System.Logger;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Node extends Thread {
    private static final Logger LOG = System.getLogger(Node.class.getName());

    private static final int NO_LEADER = -1;

    private final int nodeId;
    private final BlockingQueue<Message> inbox = new LinkedBlockingQueue<>();

    private List<Node> clusterNodes = Collections.emptyList();
    private NodeStatus status = NodeStatus.FOLLOWER;

    public Node(int nodeId) {
        super("node-" + nodeId);
        this.nodeId = nodeId;
    }

    public int getNodeId() {
        return nodeId;
    }

    public synchronized NodeStatus getNodeStatus() {
        return status;
    }

    public synchronized void setNodeStatus(NodeStatus status) {
        if (status == null) {
            throw new IllegalArgumentException("node status is null");
        }

        this.status = status;
    }

    public synchronized void setClusterNodes(List<Node> clusterNodes) {
        if (clusterNodes == null) {
            throw new IllegalArgumentException("cluster nodes is null");
        }

        this.clusterNodes = clusterNodes;
    }

    public void receive(Message message) {
        if (message == null) {
            return;
        }

        inbox.offer(message);
    }

    public void sendMessage(int toId, MessageType type) {
        sendMessage(toId, type, NO_LEADER);
    }

    public void sendMessage(int toId, MessageType type, int leaderId) {
        Node target = findNode(toId);

        if (target == null) {
            LOG.log(Logger.Level.WARNING, getName() + " cannot find node " + toId);
            return;
        }

        Message message = new Message(type, nodeId, toId, leaderId);
        LOG.log(Logger.Level.INFO, getName() + " sends " + message);
        target.receive(message);
    }

    public int getInboxSize() {
        return inbox.size();
    }

    @Override
    public void run() {
        LOG.log(Logger.Level.INFO, getName() + " started");

        while (!isInterrupted()) {
            try {
                Message message = inbox.take();
                handleMessage(message);
            } catch (InterruptedException e) {
                interrupt();
            }
        }

        LOG.log(Logger.Level.INFO, getName() + " stopped");
    }

    private void handleMessage(Message message) {
        LOG.log(Logger.Level.INFO, getName() + " got " + message);

        if (message.getType() == MessageType.PING) {
            sendMessage(message.getFromId(), MessageType.ANSWER);
        }
    }

    private synchronized Node findNode(int nodeId) {
        for (Node node : clusterNodes) {
            if (node.getNodeId() == nodeId) {
                return node;
            }
        }

        return null;
    }
}
