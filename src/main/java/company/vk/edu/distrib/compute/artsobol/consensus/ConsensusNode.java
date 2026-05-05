package company.vk.edu.distrib.compute.artsobol.consensus;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

final class ConsensusNode extends Thread {
    private static final int NO_REQUEST = 0;
    private static final long NO_DEADLINE = 0L;
    private static final long LOOP_WAIT_MS = 25L;

    private final int nodeIdentifier;
    private final ConsensusCluster cluster;
    private final ConsensusConfig config;
    private final BlockingQueue<Message> inbox = new LinkedBlockingQueue<>();
    private final AtomicBoolean activeFlag = new AtomicBoolean(true);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean electionRequested = new AtomicBoolean(true);
    private final AtomicInteger currentLeaderId = new AtomicInteger(ConsensusCluster.NO_LEADER);
    private final FailureController failureController;

    private int requestSequence;
    private int electionRequestId;
    private int pingRequestId;
    private long electionDeadlineNanos;
    private long victoryDeadlineNanos;
    private long pingDeadlineNanos;
    private long nextPingNanos;
    private boolean electionInProgress;

    ConsensusNode(int nodeId, ConsensusCluster cluster, ConsensusConfig config) {
        super("consensus-node-" + nodeId);
        this.nodeIdentifier = nodeId;
        this.cluster = cluster;
        this.config = config;
        this.failureController = new FailureController(nodeId, cluster, config.failurePolicy());
    }

    int nodeId() {
        return nodeIdentifier;
    }

    boolean active() {
        return activeFlag.get();
    }

    NodeState state() {
        boolean nodeActive = active();
        NodeRole role = nodeRole(nodeActive);
        return new NodeState(nodeIdentifier, nodeActive, role, currentLeaderId.get());
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
        if (activeFlag.compareAndSet(true, false)) {
            inbox.clear();
            currentLeaderId.set(ConsensusCluster.NO_LEADER);
            electionInProgress = false;
            electionRequested.set(false);
            pingRequestId = NO_REQUEST;
            electionRequestId = NO_REQUEST;
            victoryDeadlineNanos = NO_DEADLINE;
        }
    }

    void restore() {
        if (activeFlag.compareAndSet(false, true)) {
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
            pingRequestId = NO_REQUEST;
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
        if (cluster.leaderId() == nodeIdentifier) {
            cluster.send(message.senderId(), Message.answer(nodeIdentifier, message.requestId(), MessageType.PING));
        }
    }

    private void handleElect(Message message) {
        if (nodeIdentifier > message.senderId()) {
            cluster.send(message.senderId(), Message.answer(nodeIdentifier, message.requestId(), MessageType.ELECT));
            electionRequested.set(true);
        }
    }

    private void handleAnswer(Message message) {
        if (message.responseTo() == MessageType.PING && message.requestId() == pingRequestId) {
            pingRequestId = NO_REQUEST;
            return;
        }
        if (message.responseTo() == MessageType.ELECT && message.requestId() == electionRequestId) {
            electionInProgress = false;
            electionRequestId = NO_REQUEST;
            victoryDeadlineNanos = System.nanoTime() + config.electionTimeout().toNanos() * 2L;
        }
    }

    private void handleVictory(Message message) {
        int leaderId = message.leaderId();
        if (cluster.isHighestActive(leaderId)) {
            currentLeaderId.set(leaderId);
            electionInProgress = false;
            electionRequestId = NO_REQUEST;
            victoryDeadlineNanos = NO_DEADLINE;
            pingRequestId = NO_REQUEST;
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
                && victoryDeadlineNanos != NO_DEADLINE
                && now >= victoryDeadlineNanos) {
            startElection(now);
        }
        checkLeader(now);
    }

    private void checkLeader(long now) {
        if (cluster.leaderId() == nodeIdentifier || electionInProgress) {
            return;
        }
        if (cluster.leaderId() == ConsensusCluster.NO_LEADER) {
            startElection(now);
            return;
        }
        if (pingRequestId != NO_REQUEST && now >= pingDeadlineNanos) {
            pingRequestId = NO_REQUEST;
            currentLeaderId.set(ConsensusCluster.NO_LEADER);
            startElection(now);
            return;
        }
        if (pingRequestId == NO_REQUEST && now >= nextPingNanos) {
            pingRequestId = nextRequestId();
            pingDeadlineNanos = now + config.pingTimeout().toNanos();
            nextPingNanos = now + config.pingInterval().toNanos();
            cluster.send(cluster.leaderId(), Message.ping(nodeIdentifier, pingRequestId));
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
        pingRequestId = NO_REQUEST;

        if (!cluster.hasHigherActiveNode(nodeIdentifier)) {
            declareVictory();
            return;
        }
        for (int higherId : cluster.higherNodeIds(nodeIdentifier)) {
            cluster.send(higherId, Message.elect(nodeIdentifier, electionRequestId));
        }
    }

    private void declareVictory() {
        electionInProgress = false;
        electionRequestId = NO_REQUEST;
        if (cluster.publishVictory(nodeIdentifier)) {
            currentLeaderId.set(nodeIdentifier);
            victoryDeadlineNanos = NO_DEADLINE;
        } else {
            currentLeaderId.set(ConsensusCluster.NO_LEADER);
            electionRequested.set(true);
        }
    }

    private int nextRequestId() {
        requestSequence++;
        if (requestSequence == NO_REQUEST) {
            requestSequence++;
        }
        return requestSequence;
    }

    private NodeRole nodeRole(boolean nodeActive) {
        if (!nodeActive) {
            return NodeRole.DOWN;
        }
        if (cluster.leaderId() == nodeIdentifier && currentLeaderId.get() == nodeIdentifier) {
            return NodeRole.LEADER;
        }
        return NodeRole.FOLLOWER;
    }
}
