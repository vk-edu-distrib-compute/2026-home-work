package company.vk.edu.distrib.compute.tadzhnahal.consensus;

import java.lang.System.Logger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Node extends Thread {
    public static final int NO_LEADER = -1;

    private static final Logger LOG = System.getLogger(Node.class.getName());

    private final int nodeId;
    private final BlockingQueue<Message> inbox = new LinkedBlockingQueue<>();
    private final Election election = new Election(this);

    private List<Node> clusterNodes = Collections.emptyList();
    private NodeStatus status = NodeStatus.FOLLOWER;
    private int leaderId = NO_LEADER;
    private boolean electionInProgress;
    private boolean answerFromHigherNode;

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

    public synchronized int getLeaderId() {
        return leaderId;
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

    public void startElection() {
        election.start();
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

    synchronized boolean tryStartElection() {
        if (electionInProgress) {
            return false;
        }

        electionInProgress = true;
        answerFromHigherNode = false;
        status = NodeStatus.FOLLOWER;
        leaderId = NO_LEADER;

        return true;
    }

    synchronized void finishElection() {
        electionInProgress = false;
    }

    synchronized void rememberAnswerFromHigherNode() {
        answerFromHigherNode = true;
    }

    synchronized boolean hasAnswerFromHigherNode() {
        return answerFromHigherNode;
    }

    synchronized void rememberLeader(int leaderId) {
        this.leaderId = leaderId;

        if (leaderId == nodeId) {
            status = NodeStatus.LEADER;
            return;
        }

        status = NodeStatus.FOLLOWER;
    }

    synchronized List<Node> getClusterNodesSnapshot() {
        return new ArrayList<>(clusterNodes);
    }

    private void handleMessage(Message message) {
        LOG.log(Logger.Level.INFO, getName() + " got " + message);

        if (message.getType() == MessageType.PING) {
            handlePing(message);
            return;
        }

        if (message.getType() == MessageType.ELECT) {
            handleElect(message);
            return;
        }

        if (message.getType() == MessageType.ANSWER) {
            handleAnswer(message);
            return;
        }

        if (message.getType() == MessageType.VICTORY) {
            handleVictory(message);
        }
    }

    private void handlePing(Message message) {
        sendMessage(message.getFromId(), MessageType.ANSWER);
    }

    private void handleElect(Message message) {
        if (message.getFromId() < nodeId) {
            sendMessage(message.getFromId(), MessageType.ANSWER);
            startElection();
        }
    }

    private void handleAnswer(Message message) {
        rememberAnswerFromHigherNode();
        LOG.log(Logger.Level.INFO, getName() + " got answer from node " + message.getFromId());
    }

    private void handleVictory(Message message) {
        rememberLeader(message.getLeaderId());
        LOG.log(Logger.Level.INFO, getName() + " accepts leader " + message.getLeaderId());
    }

    private Node findNode(int nodeId) {
        for (Node node : getClusterNodesSnapshot()) {
            if (node.getNodeId() == nodeId) {
                return node;
            }
        }

        return null;
    }
}
