package company.vk.edu.distrib.compute.artsobol.consensus;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

final class ConsensusNode extends Thread {
    private static final long LOOP_WAIT_MS = 25L;

    private final int nodeId;
    private final ConsensusCluster cluster;
    private final ConsensusConfig config;
    private final BlockingQueue<Message> inbox = new LinkedBlockingQueue<>();
    private final AtomicBoolean active = new AtomicBoolean(true);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean electionRequested = new AtomicBoolean(true);
    private final AtomicInteger currentLeaderId = new AtomicInteger(ConsensusCluster.NO_LEADER);
    private final FailureController failureController;

    private int nextRequestId;
    private int electionRequestId;
    private int pingRequestId;
    private long electionDeadlineNanos;
    private long victoryDeadlineNanos;
    private long pingDeadlineNanos;
    private long nextPingNanos;
    private boolean electionInProgress;

    ConsensusNode(int nodeId, ConsensusCluster cluster, ConsensusConfig config) {
        super("consensus-node-" + nodeId);
        this.nodeId = nodeId;
        this.cluster = cluster;
        this.config = config;
        this.failureController = new FailureController(nodeId, cluster, config.failurePolicy());
    }

    int nodeId() {
        return nodeId;
    }

    boolean active() {
        return active.get();
    }

    NodeState state() {
        boolean nodeActive = active();
        NodeRole role = nodeRole(nodeActive);
        return new NodeState(nodeId, nodeActive, role, currentLeaderId.get());
    }

    void receive(Message message) {
        if (active()) {
            inbox.add(message);
        }
    }

    void requestElection() {
        electionRequested.set(true);
    }

    void fail() {
        if (active.compareAndSet(true, false)) {
            inbox.clear();
            currentLeaderId.set(ConsensusCluster.NO_LEADER);
            electionInProgress = false;
            electionRequested.set(false);
            pingRequestId = 0;
            electionRequestId = 0;
            victoryDeadlineNanos = 0L;
        }
    }

    void restore() {
        if (active.compareAndSet(false, true)) {
            inbox.clear();
            currentLeaderId.set(ConsensusCluster.NO_LEADER);
            failureController.resetRecovery();
            electionRequested.set(true);
        }
    }

    void stopNode() {
        running.set(false);
        interrupt();
    }

    void shutdownGracefully() {
        fail();
        stopNode();
    }

    void notifyLeaderShutdown(int leaderId) {
        if (active() && (currentLeaderId.get() == leaderId || cluster.leaderId() == leaderId)) {
            currentLeaderId.set(ConsensusCluster.NO_LEADER);
            pingRequestId = 0;
            electionRequested.set(true);
        }
    }

    @Override
    public void run() {
        while (running.get()) {
            try {
                runIteration();
            } catch (InterruptedException e) {
                interrupt();
                running.set(false);
            }
        }
    }

    private void runIteration() throws InterruptedException {
        if (!active()) {
            failureController.recoverIfDue(System.nanoTime());
            TimeUnit.MILLISECONDS.sleep(LOOP_WAIT_MS);
            return;
        }

        Message message = inbox.poll(LOOP_WAIT_MS, TimeUnit.MILLISECONDS);
        if (message != null) {
            handle(message);
        }
        long now = System.nanoTime();
        failureController.failIfDue(now);
        if (active()) {
            runTimers(now);
        }
    }

    private void handle(Message message) {
        switch (message.type()) {
            case PING -> handlePing(message);
            case ELECT -> handleElect(message);
            case ANSWER -> handleAnswer(message);
            case VICTORY -> handleVictory(message);
        }
    }

    private void handlePing(Message message) {
        if (cluster.leaderId() == nodeId) {
            cluster.send(message.senderId(), Message.answer(nodeId, message.requestId(), MessageType.PING));
        }
    }

    private void handleElect(Message message) {
        if (nodeId > message.senderId()) {
            cluster.send(message.senderId(), Message.answer(nodeId, message.requestId(), MessageType.ELECT));
            electionRequested.set(true);
        }
    }

    private void handleAnswer(Message message) {
        if (message.responseTo() == MessageType.PING && message.requestId() == pingRequestId) {
            pingRequestId = 0;
            return;
        }
        if (message.responseTo() == MessageType.ELECT && message.requestId() == electionRequestId) {
            electionInProgress = false;
            electionRequestId = 0;
            victoryDeadlineNanos = System.nanoTime() + config.electionTimeout().toNanos() * 2L;
        }
    }

    private void handleVictory(Message message) {
        int leaderId = message.leaderId();
        if (cluster.isHighestActive(leaderId)) {
            currentLeaderId.set(leaderId);
            electionInProgress = false;
            electionRequestId = 0;
            victoryDeadlineNanos = 0L;
            pingRequestId = 0;
            nextPingNanos = System.nanoTime() + config.pingInterval().toNanos();
        } else {
            electionRequested.set(true);
        }
    }

    private void runTimers(long now) {
        if (electionRequested.getAndSet(false)) {
            startElection(now);
        }
        if (electionInProgress && now >= electionDeadlineNanos) {
            declareVictory();
        }
        if (!electionInProgress
                && currentLeaderId.get() == ConsensusCluster.NO_LEADER
                && victoryDeadlineNanos > 0L
                && now >= victoryDeadlineNanos) {
            startElection(now);
        }
        checkLeader(now);
    }

    private void checkLeader(long now) {
        if (cluster.leaderId() == nodeId || electionInProgress) {
            return;
        }
        if (cluster.leaderId() == ConsensusCluster.NO_LEADER) {
            startElection(now);
            return;
        }
        if (pingRequestId != 0 && now >= pingDeadlineNanos) {
            pingRequestId = 0;
            currentLeaderId.set(ConsensusCluster.NO_LEADER);
            startElection(now);
            return;
        }
        if (pingRequestId == 0 && now >= nextPingNanos) {
            pingRequestId = nextRequestId();
            pingDeadlineNanos = now + config.pingTimeout().toNanos();
            nextPingNanos = now + config.pingInterval().toNanos();
            cluster.send(cluster.leaderId(), Message.ping(nodeId, pingRequestId));
        }
    }

    private void startElection(long now) {
        if (!active() || electionInProgress) {
            return;
        }
        electionInProgress = true;
        electionRequestId = nextRequestId();
        electionDeadlineNanos = now + config.electionTimeout().toNanos();
        currentLeaderId.set(ConsensusCluster.NO_LEADER);
        pingRequestId = 0;

        if (!cluster.hasHigherActiveNode(nodeId)) {
            declareVictory();
            return;
        }
        for (int higherId : cluster.higherNodeIds(nodeId)) {
            cluster.send(higherId, Message.elect(nodeId, electionRequestId));
        }
    }

    private void declareVictory() {
        electionInProgress = false;
        electionRequestId = 0;
        if (cluster.publishVictory(nodeId)) {
            currentLeaderId.set(nodeId);
            victoryDeadlineNanos = 0L;
        } else {
            currentLeaderId.set(ConsensusCluster.NO_LEADER);
            electionRequested.set(true);
        }
    }

    private int nextRequestId() {
        nextRequestId++;
        if (nextRequestId == 0) {
            nextRequestId++;
        }
        return nextRequestId;
    }

    private NodeRole nodeRole(boolean nodeActive) {
        if (!nodeActive) {
            return NodeRole.DOWN;
        }
        if (cluster.leaderId() == nodeId && currentLeaderId.get() == nodeId) {
            return NodeRole.LEADER;
        }
        return NodeRole.FOLLOWER;
    }
}
