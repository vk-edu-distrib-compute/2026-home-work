package company.vk.edu.distrib.compute.tadzhnahal.consensus;

import java.lang.System.Logger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Node extends Thread {
    public static final int NO_LEADER = -1;

    private static final Logger LOG = System.getLogger(Node.class.getName());

    private static final long LOOP_WAIT_MS = 200L;

    private final int nodeId;
    private final BlockingQueue<Message> inbox = new LinkedBlockingQueue<>();
    private final Election election = new Election(this);
    private final LeaderMonitor leaderMonitor = new LeaderMonitor(this);
    private final NodeMessageHandler messageHandler = new NodeMessageHandler(this);

    private List<Node> clusterNodes = Collections.emptyList();
    private NodeStatus status = NodeStatus.FOLLOWER;
    private int leaderId = NO_LEADER;
    private long lastLeaderAnswerTime = System.currentTimeMillis();
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

    public synchronized long getLastLeaderAnswerTime() {
        return lastLeaderAnswerTime;
    }

    public synchronized boolean isWorking() {
        return status != NodeStatus.DOWN;
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

    public synchronized void turnOff() {
        LOG.log(Logger.Level.INFO, getName() + " turns off");

        status = NodeStatus.DOWN;
        leaderId = NO_LEADER;
        electionInProgress = false;
        answerFromHigherNode = false;
        inbox.clear();
    }

    public synchronized void turnOn() {
        if (status != NodeStatus.DOWN) {
            LOG.log(Logger.Level.INFO, getName() + " is already alive");
            return;
        }

        LOG.log(Logger.Level.INFO, getName() + " turns on");

        status = NodeStatus.FOLLOWER;
        leaderId = NO_LEADER;
        lastLeaderAnswerTime = System.currentTimeMillis();
        electionInProgress = false;
        answerFromHigherNode = false;
        inbox.clear();

        startElection();
    }

    public void receive(Message message) {
        if (message == null) {
            return;
        }

        if (!isWorking()) {
            return;
        }

        inbox.offer(message);
    }

    public void sendMessage(int toId, MessageType type) {
        sendMessage(toId, type, NO_LEADER);
    }

    public void sendMessage(int toId, MessageType type, int leaderId) {
        if (!isWorking()) {
            return;
        }

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
                Message message = inbox.poll(LOOP_WAIT_MS, TimeUnit.MILLISECONDS);

                if (message != null) {
                    messageHandler.handle(message);
                }

                leaderMonitor.tick();
            } catch (InterruptedException e) {
                interrupt();
            }
        }

        LOG.log(Logger.Level.INFO, getName() + " stopped");
    }

    synchronized boolean tryStartElection() {
        if (!isWorking() || electionInProgress) {
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
        if (!isWorking()) {
            return;
        }

        this.leaderId = leaderId;
        lastLeaderAnswerTime = System.currentTimeMillis();

        if (leaderId == nodeId) {
            status = NodeStatus.LEADER;
            return;
        }

        status = NodeStatus.FOLLOWER;
    }

    synchronized void rememberLeaderAnswer() {
        lastLeaderAnswerTime = System.currentTimeMillis();
    }

    synchronized void clearLeader() {
        if (status != NodeStatus.DOWN) {
            leaderId = NO_LEADER;
            status = NodeStatus.FOLLOWER;
        }
    }

    synchronized List<Node> getClusterNodesSnapshot() {
        return new ArrayList<>(clusterNodes);
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
