package company.vk.edu.distrib.compute.tadzhnahal.consensus;

import java.lang.System.Logger;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Node extends Thread {
    private static final Logger LOG = System.getLogger(Node.class.getName());

    private final int nodeId;
    private final BlockingQueue<Message> inbox = new LinkedBlockingQueue<>();

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

    public void receive(Message message) {
        if (message == null) {
            return;
        }

        inbox.offer(message);
    }

    public int getInboxSize() {
        return inbox.size();
    }

    @Override
    public void run() {
        while (!isInterrupted()) {
            try {
                Message message = inbox.take();
                LOG.log(Logger.Level.INFO, getName() + " got " + message);
            } catch (InterruptedException e) {
                interrupt();
            }
        }
    }
}
