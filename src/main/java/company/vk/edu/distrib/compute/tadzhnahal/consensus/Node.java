package company.vk.edu.distrib.compute.tadzhnahal.consensus;

import java.lang.System.Logger;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Node extends Thread {
    static final int NO_LEADER = -1;

    private static final Logger LOG = System.getLogger(Node.class.getName());
    private static final long POLL_TIMEOUT_MS = 100L;

    private final int nodeId;
    private final Cluster cluster;
    private final BlockingQueue<Message> inbox = new LinkedBlockingQueue<>();
    private final NodeMessageHandler messageHandler = new NodeMessageHandler(this);
    private final LeaderMonitor leaderMonitor = new LeaderMonitor(this);

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean working = new AtomicBoolean(true);
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);
    private final AtomicBoolean gotAnswerFromHigher = new AtomicBoolean(false);
    private final AtomicInteger leaderId = new AtomicInteger(NO_LEADER);
    private final AtomicLong lastLeaderAnswerTime = new AtomicLong(System.currentTimeMillis());

    public Node(int nodeId, Cluster cluster) {
        super("node-" + nodeId);

        if (nodeId <= 0) {
            throw new IllegalArgumentException("node id must be positive");
        }
        if (cluster == null) {
            throw new IllegalArgumentException("cluster is null");
        }

        this.nodeId = nodeId;
        this.cluster = cluster;
    }

    public int getNodeId() {
        return nodeId;
    }

    public int getLeaderId() {
        return leaderId.get();
    }

    public long getLastLeaderAnswerTime() {
        return lastLeaderAnswerTime.get();
    }

    public boolean isWorking() {
        return working.get();
    }

    public boolean isElectionInProgress() {
        return electionInProgress.get();
    }

    public boolean hasAnswerFromHigher() {
        return gotAnswerFromHigher.get();
    }

    public int getInboxSize() {
        return inbox.size();
    }

    public void turnOff() {
        if (working.compareAndSet(true, false)) {
            leaderId.set(NO_LEADER);
            gotAnswerFromHigher.set(false);
            electionInProgress.set(false);
            inbox.clear();

            LogHelper.info(LOG, () -> getName() + " turns off");
        }
    }

    public void turnOn() {
        if (working.compareAndSet(false, true)) {
            leaderId.set(NO_LEADER);
            lastLeaderAnswerTime.set(System.currentTimeMillis());

            LogHelper.info(LOG, () -> getName() + " turns on");
            startElection();
        }
    }

    public void shutdown() {
        running.set(false);
        receiveMessage(new Message(MessageType.SHUTDOWN, nodeId, nodeId, NO_LEADER));
    }

    public void receiveMessage(Message message) {
        if (message == null) {
            return;
        }

        if (!running.get() && message.getType() != MessageType.SHUTDOWN) {
            return;
        }

        if (!working.get() && message.getType() != MessageType.SHUTDOWN) {
            return;
        }

        inbox.offer(message);
    }

    public void sendMessage(int toId, MessageType type) {
        sendMessage(toId, type, NO_LEADER);
    }

    public void sendMessage(int toId, MessageType type, int newLeaderId) {
        if (!working.get()) {
            return;
        }

        Message message = new Message(type, nodeId, toId, newLeaderId);
        LogHelper.info(LOG, () -> getName() + " sends " + message);

        cluster.getNode(toId).receiveMessage(message);
    }

    public void startElection() {
        if (!working.get()) {
            return;
        }

        if (electionInProgress.compareAndSet(false, true)) {
            gotAnswerFromHigher.set(false);
            new Election(this).start();
        }
    }

    public void finishElection() {
        electionInProgress.set(false);
    }

    public void markAnswerFromHigher(int fromId) {
        if (fromId > nodeId) {
            gotAnswerFromHigher.set(true);
        }

        if (fromId == leaderId.get()) {
            lastLeaderAnswerTime.set(System.currentTimeMillis());
        }
    }

    public void acceptLeader(int newLeaderId) {
        leaderId.set(newLeaderId);
        lastLeaderAnswerTime.set(System.currentTimeMillis());
        finishElection();
    }

    public void clearLeader() {
        leaderId.set(NO_LEADER);
    }

    public void becomeLeader() {
        leaderId.set(nodeId);
        lastLeaderAnswerTime.set(System.currentTimeMillis());
        finishElection();
    }

    public boolean isNodeWorking(int checkedNodeId) {
        return cluster.getNode(checkedNodeId).isWorking();
    }

    public boolean hasHigherNode(int checkedNodeId) {
        return checkedNodeId > nodeId;
    }

    public void sendVictoryToAll() {
        for (Node node : cluster.getNodes()) {
            if (node.getNodeId() != nodeId) {
                sendMessage(node.getNodeId(), MessageType.VICTORY, nodeId);
            }
        }
    }

    public void sendElectToHigherNodes() {
        for (Node node : cluster.getNodes()) {
            if (node.getNodeId() > nodeId) {
                sendMessage(node.getNodeId(), MessageType.ELECT);
            }
        }
    }

    public String getStatus() {
        if (!working.get()) {
            return "DOWN";
        }

        if (leaderId.get() == nodeId) {
            return "LEADER";
        }

        return "FOLLOWER";
    }

    @Override
    public void run() {
        LogHelper.info(LOG, () -> getName() + " started");

        while (running.get()) {
            try {
                Message message = inbox.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);

                if (message != null) {
                    if (message.getType() == MessageType.SHUTDOWN) {
                        break;
                    }

                    messageHandler.handle(message);
                }

                leaderMonitor.tick();
            } catch (InterruptedException e) {
                currentThread().interrupt();
                break;
            }
        }

        LogHelper.info(LOG, () -> getName() + " stopped");
    }
}
