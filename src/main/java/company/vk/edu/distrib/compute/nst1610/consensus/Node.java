package company.vk.edu.distrib.compute.nst1610.consensus;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Node extends Thread {
    private static final Logger log = LoggerFactory.getLogger(Node.class);
    private final int nodeId;
    private final BlockingQueue<Message> messages = new LinkedBlockingQueue<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Object stateLock = new Object();
    private final ConsensusConfig config;
    private final ElectionCoordinator electionCoordinator;
    private final Random random;
    private volatile Map<Integer, Node> cluster = Map.of();
    private volatile boolean failed;
    private volatile boolean leader;
    private volatile int leaderId = -1;
    private volatile long leaderClock;
    private volatile long forcedRecoveryAt = -1L;
    private volatile long nextFailureCheckAt;
    private volatile long nextPingAt;
    private volatile long pingDeadlineAt = -1L;
    private volatile boolean waitingPingAnswer;
    private volatile long activeElectionClock = -1L;
    private volatile long electionDeadlineAt = -1L;
    private volatile boolean waitingElectionAnswer;
    private volatile boolean waitingVictory;

    public Node(int nodeId, ConsensusConfig config, ElectionCoordinator electionCoordinator) {
        super("consensus-node-" + nodeId);
        this.nodeId = nodeId;
        this.config = config;
        this.electionCoordinator = electionCoordinator;
        this.random = new Random();
    }

    public void setCluster(Map<Integer, Node> cluster) {
        this.cluster = Map.copyOf(cluster);
    }

    public void submitMessage(Message message) {
        if (!running.get() || failed) {
            return;
        }
        messages.offer(message);
    }

    public boolean isFailed() {
        return failed;
    }

    public void failNode() {
        synchronized (stateLock) {
            if (failed) {
                return;
            }
            failed = true;
            leader = false;
            leaderId = -1;
            waitingPingAnswer = false;
            waitingElectionAnswer = false;
            waitingVictory = false;
            pingDeadlineAt = -1L;
            electionDeadlineAt = -1L;
            forcedRecoveryAt = -1L;
            messages.clear();
            log.info("Node {} failed", nodeId);
        }
    }

    public void recoverNode() {
        synchronized (stateLock) {
            if (!failed) {
                return;
            }
            failed = false;
            leader = false;
            leaderId = -1;
            waitingPingAnswer = false;
            waitingElectionAnswer = false;
            waitingVictory = false;
            pingDeadlineAt = -1L;
            electionDeadlineAt = -1L;
            nextPingAt = 0L;
            nextFailureCheckAt = System.currentTimeMillis() + config.failureCheckIntervalMillis();
            forcedRecoveryAt = -1L;
            log.info("Node {} recovered", nodeId);
        }
        startElection(ElectionReason.RECOVERY);
    }

    public void scheduleRecovery(long delayMillis) {
        synchronized (stateLock) {
            forcedRecoveryAt = System.currentTimeMillis() + delayMillis;
        }
    }

    public void shutdownNode() {
        running.set(false);
        interrupt();
    }

    public NodeSnapshot snapshot() {
        return new NodeSnapshot(nodeId, failed, leader && !failed, leaderId, leaderClock);
    }

    @Override
    public void run() {
        nextPingAt = System.currentTimeMillis();
        nextFailureCheckAt = System.currentTimeMillis() + config.failureCheckIntervalMillis();
        startElection(ElectionReason.START);
        while (running.get()) {
            try {
                Message message = messages.poll(config.loopDelayMillis(), TimeUnit.MILLISECONDS);
                if (message != null) {
                    handleMessage(message);
                }
                tick();
            } catch (InterruptedException e) {
                if (!running.get()) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void tick() {
        long now = System.currentTimeMillis();
        if (failed) {
            if (forcedRecoveryAt > 0 && now >= forcedRecoveryAt) {
                recoverNode();
            }
            return;
        }
        simulateFailure(now);
        processCoordinatorState();
        processPing(now);
        processElectionTimeout(now);
    }

    private void simulateFailure(long now) {
        if (config.failureProbability() <= 0 || now < nextFailureCheckAt) {
            return;
        }
        nextFailureCheckAt = now + config.failureCheckIntervalMillis();
        if (random.nextDouble() >= config.failureProbability()) {
            return;
        }
        long delay = config.recoveryDelayMinMillis();
        if (config.recoveryDelayMaxMillis() > config.recoveryDelayMinMillis()) {
            delay += random.nextLong(config.recoveryDelayMaxMillis() - config.recoveryDelayMinMillis() + 1);
        }
        failNode();
        scheduleRecovery(delay);
    }

    private void processCoordinatorState() {
        long committedClock = electionCoordinator.getCommittedClock();
        int committedLeader = electionCoordinator.getCommittedLeader();
        if (committedClock > leaderClock && committedLeader > 0) {
            acceptLeader(committedLeader, committedClock);
        }
    }

    private void processPing(long now) {
        if (leader) {
            return;
        }
        if (waitingElectionAnswer || waitingVictory) {
            return;
        }
        if (leaderId < 0) {
            startElection(ElectionReason.LEADER_MISSING);
            return;
        }
        if (waitingPingAnswer && now >= pingDeadlineAt) {
            waitingPingAnswer = false;
            log.info("Node {} detected leader {} timeout", nodeId, leaderId);
            startElection(ElectionReason.LEADER_TIMEOUT);
            return;
        }
        if (!waitingPingAnswer && now >= nextPingAt) {
            Node leaderNode = cluster.get(leaderId);
            if (leaderNode == null || leaderNode.isFailed()) {
                startElection(ElectionReason.LEADER_UNAVAILABLE);
                return;
            }
            sendMessage(leaderId, Message.ping(nodeId, leaderClock, leaderId));
            waitingPingAnswer = true;
            pingDeadlineAt = now + config.pingTimeoutMillis();
            nextPingAt = now + config.pingIntervalMillis();
        }
    }

    private void processElectionTimeout(long now) {
        if (waitingElectionAnswer && now >= electionDeadlineAt) {
            waitingElectionAnswer = false;
            declareVictory(activeElectionClock);
            return;
        }
        if (waitingVictory && now >= electionDeadlineAt) {
            waitingVictory = false;
            startElection(ElectionReason.LEADER_MISSING);
        }
    }

    private void handleMessage(Message message) {
        if (failed) {
            return;
        }
        switch (message.type()) {
            case PING -> handlePing(message);
            case ELECT -> handleElect(message);
            case ANSWER -> handleAnswer(message);
            case VICTORY -> handleVictory(message);
            default -> throw new IllegalStateException("Unsupported message: " + message.type());
        }
    }

    private void handlePing(Message message) {
        if (!leader) {
            return;
        }
        sendMessage(message.senderId(), Message.answer(nodeId, leaderClock, ResponseKind.PING));
    }

    private void handleElect(Message message) {
        if (message.senderId() >= nodeId) {
            return;
        }
        sendMessage(message.senderId(), Message.answer(nodeId, message.electionClock(), ResponseKind.ELECT));
        if (message.electionClock() >= leaderClock && !waitingElectionAnswer) {
            startElection(ElectionReason.ELDER_NODE_ELECT);
        }
    }

    private void handleAnswer(Message message) {
        if (message.responseKind() == ResponseKind.PING && message.senderId() == leaderId) {
            waitingPingAnswer = false;
            pingDeadlineAt = -1L;
            return;
        }
        if (message.responseKind() == ResponseKind.ELECT
            && waitingElectionAnswer
            && message.electionClock() == activeElectionClock) {
            waitingElectionAnswer = false;
            waitingVictory = true;
            electionDeadlineAt = System.currentTimeMillis() + config.electionTimeoutMillis();
        }
    }

    private void handleVictory(Message message) {
        acceptLeader(message.leaderId(), message.electionClock());
    }

    public void startElection(ElectionReason reason) {
        if (failed || !running.get()) {
            return;
        }
        synchronized (stateLock) {
            long clock = electionCoordinator.getNextElectionClock();
            activeElectionClock = clock;
            waitingPingAnswer = false;
            waitingVictory = false;
            pingDeadlineAt = -1L;
            List<Integer> higherNodes = cluster.keySet().stream()
                .filter(otherId -> otherId > nodeId)
                .sorted()
                .toList();
            if (higherNodes.isEmpty()) {
                log.info("Node {} starts election {} and has max id", nodeId, clock);
                declareVictory(clock);
                return;
            }
            waitingElectionAnswer = true;
            electionDeadlineAt = System.currentTimeMillis() + config.electionTimeoutMillis();
            log.info("Node {} starts election on clock {} because {}", nodeId, clock, reason);
            for (int higherId : higherNodes) {
                sendMessage(higherId, Message.elect(nodeId, clock));
            }
        }
    }

    private void declareVictory(long clock) {
        if (failed) {
            return;
        }
        if (!electionCoordinator.tryCommitVictory(clock, nodeId)) {
            processCoordinatorState();
            return;
        }
        acceptLeader(nodeId, clock);
        for (int otherId : cluster.keySet()) {
            if (otherId == nodeId) {
                continue;
            }
            sendMessage(otherId, Message.victory(nodeId, clock));
        }
        log.info("Node {} became leader on clock {}", nodeId, clock);
    }

    private void acceptLeader(int leaderId, long clock) {
        if (leaderId < 0 || clock < leaderClock) {
            return;
        }
        synchronized (stateLock) {
            if (clock < leaderClock) {
                return;
            }
            this.leaderId = leaderId;
            leaderClock = clock;
            leader = leaderId == nodeId;
            waitingElectionAnswer = false;
            waitingVictory = false;
            waitingPingAnswer = false;
            electionDeadlineAt = -1L;
            pingDeadlineAt = -1L;
            nextPingAt = System.currentTimeMillis() + config.pingIntervalMillis();
            log.info("Node {} accepted leader {} on clock {}", nodeId, leaderId, clock);
        }
    }

    private void sendMessage(int targetNodeId, Message message) {
        if (failed) {
            return;
        }
        Node target = cluster.get(targetNodeId);
        if (target != null) {
            target.submitMessage(message);
        }
    }

    public static List<NodeSnapshot> sortSnapshots(List<NodeSnapshot> snapshots) {
        List<NodeSnapshot> result = new ArrayList<>(snapshots);
        result.sort(Comparator.comparingInt(NodeSnapshot::nodeId));
        return result;
    }
}
