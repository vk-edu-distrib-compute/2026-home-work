package company.vk.edu.distrib.compute.che1nov.consensus;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class ClusterNode extends Thread {
    private static final int NO_LEADER = -1;

    private final int id;
    private final BlockingQueue<ClusterMessage> inbox = new LinkedBlockingQueue<>();
    private final Map<Integer, ClusterNode> peers = new ConcurrentHashMap<>();
    private final ReentrantLock stateLock = new ReentrantLock();

    private final long pingIntervalMillis;
    private final long pingTimeoutMillis;
    private final long electionTimeoutMillis;

    private final double failProbability;
    private final long minRecoverMillis;
    private final long maxRecoverMillis;

    private final AtomicBoolean running = new AtomicBoolean(true);

    private boolean alive = true;
    private NodeRole currentRole = NodeRole.FOLLOWER;
    private int currentLeaderId = NO_LEADER;

    private boolean electionInProgress;
    private boolean receivedElectAnswer;
    private long electionDeadlineAt;
    private boolean waitingPingAnswer;
    private long pingDeadlineAt;
    private long nextPingAt;
    private long recoverAt;
    private boolean forceStopped;

    public ClusterNode(
            int nodeId,
            Duration pingInterval,
            Duration pingTimeout,
            Duration electionTimeout,
            double failProbability,
            Duration minRecover,
            Duration maxRecover
    ) {
        super("cluster-node-" + nodeId);
        this.id = nodeId;
        this.pingIntervalMillis = pingInterval.toMillis();
        this.pingTimeoutMillis = pingTimeout.toMillis();
        this.electionTimeoutMillis = electionTimeout.toMillis();
        this.failProbability = failProbability;
        this.minRecoverMillis = minRecover.toMillis();
        this.maxRecoverMillis = maxRecover.toMillis();
        this.nextPingAt = System.currentTimeMillis() + pingIntervalMillis;
    }

    public int nodeId() {
        return id;
    }

    public Integer leaderId() {
        stateLock.lock();
        try {
            return currentLeaderId == NO_LEADER ? null : currentLeaderId;
        } finally {
            stateLock.unlock();
        }
    }

    public NodeRole role() {
        stateLock.lock();
        try {
            return currentRole;
        } finally {
            stateLock.unlock();
        }
    }

    public boolean isAliveNode() {
        stateLock.lock();
        try {
            return alive;
        } finally {
            stateLock.unlock();
        }
    }

    public void setPeers(Map<Integer, ClusterNode> allNodes) {
        peers.clear();
        for (Map.Entry<Integer, ClusterNode> entry : allNodes.entrySet()) {
            if (entry.getKey() != id) {
                peers.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public void deliver(ClusterMessage message) {
        inbox.offer(message);
    }

    public void forceDown() {
        stateLock.lock();
        try {
            markDown(true);
        } finally {
            stateLock.unlock();
        }
    }

    private void markDown(boolean forced) {
        if (!alive) {
            return;
        }
        forceStopped = forced;
        alive = false;
        currentRole = NodeRole.DOWN;
        currentLeaderId = NO_LEADER;
        electionInProgress = false;
        waitingPingAnswer = false;
        recoverAt = forced ? 0L : System.currentTimeMillis() + randomRecoverDelay();
    }

    public void forceUp() {
        stateLock.lock();
        try {
            if (alive) {
                return;
            }
            forceStopped = false;
            alive = true;
            currentRole = NodeRole.FOLLOWER;
            currentLeaderId = NO_LEADER;
            waitingPingAnswer = false;
            startElectionLocked();
        } finally {
            stateLock.unlock();
        }
    }

    public void shutdownNode() {
        running.set(false);
        this.interrupt();
    }

    @Override
    public void run() {
        stateLock.lock();
        try {
            startElectionLocked();
        } finally {
            stateLock.unlock();
        }
        while (running.get()) {
            try {
                maybeSimulateFailure();
                maybeAutoRecover();

                if (alive) {
                    processOneMessage();
                    checkElectionTimeout();
                    maybePingLeader();
                    checkPingTimeout();
                } else {
                    TimeUnit.MILLISECONDS.sleep(50);
                }
            } catch (InterruptedException e) {
                currentThread().interrupt();
            }
        }
    }

    private void processOneMessage() throws InterruptedException {
        ClusterMessage message = inbox.poll(50, TimeUnit.MILLISECONDS);
        if (message == null) {
            return;
        }
        switch (message.type()) {
            case PING -> onPing(message.fromId());
            case ELECT -> onElect(message.fromId());
            case ANSWER -> onAnswer(message.fromId(), message.answerKind());
            case VICTORY -> onVictory(message.fromId());
        }
    }

    private void onPing(int fromId) {
        stateLock.lock();
        try {
            if (!alive) {
                return;
            }
        } finally {
            stateLock.unlock();
        }
        send(fromId, ClusterMessage.answer(id, AnswerKind.PING));
    }

    private void onElect(int fromId) {
        boolean shouldElect = false;
        stateLock.lock();
        try {
            if (!alive) {
                return;
            }
            if (id > fromId) {
                shouldElect = true;
            }
        } finally {
            stateLock.unlock();
        }
        if (shouldElect) {
            send(fromId, ClusterMessage.answer(id, AnswerKind.ELECT));
            stateLock.lock();
            try {
                startElectionLocked();
            } finally {
                stateLock.unlock();
            }
        }
    }

    private void onAnswer(int fromId, AnswerKind answerKind) {
        stateLock.lock();
        try {
            if (!alive) {
                return;
            }
            if (answerKind == AnswerKind.PING && currentLeaderId == fromId) {
                waitingPingAnswer = false;
            }
            if (answerKind == AnswerKind.ELECT && electionInProgress) {
                receivedElectAnswer = true;
                electionDeadlineAt = System.currentTimeMillis() + electionTimeoutMillis;
            }
        } finally {
            stateLock.unlock();
        }
    }

    private void onVictory(int winnerId) {
        stateLock.lock();
        try {
            if (!alive) {
                return;
            }
            if (winnerId < id) {
                startElectionLocked();
                return;
            }
            currentLeaderId = winnerId;
            electionInProgress = false;
            receivedElectAnswer = false;
            waitingPingAnswer = false;
            currentRole = winnerId == id ? NodeRole.LEADER : NodeRole.FOLLOWER;
        } finally {
            stateLock.unlock();
        }
    }

    private void maybePingLeader() {
        long now = System.currentTimeMillis();
        Integer leaderToPing;
        stateLock.lock();
        try {
            if (now < nextPingAt) {
                return;
            }
            nextPingAt = now + pingIntervalMillis;

            if (currentRole == NodeRole.LEADER) {
                currentLeaderId = id;
                return;
            }

            if (currentLeaderId == NO_LEADER) {
                startElectionLocked();
                return;
            }
            if (waitingPingAnswer) {
                return;
            }

            leaderToPing = currentLeaderId;
            waitingPingAnswer = true;
            pingDeadlineAt = now + pingTimeoutMillis;
        } finally {
            stateLock.unlock();
        }
        send(leaderToPing, ClusterMessage.ping(id));
    }

    private void checkPingTimeout() {
        stateLock.lock();
        try {
            if (!waitingPingAnswer) {
                return;
            }
            if (System.currentTimeMillis() >= pingDeadlineAt) {
                waitingPingAnswer = false;
                startElectionLocked();
            }
        } finally {
            stateLock.unlock();
        }
    }

    private void startElectionLocked() {
        if (!alive) {
            return;
        }

        electionInProgress = true;
        receivedElectAnswer = false;
        currentRole = NodeRole.CANDIDATE;
        electionDeadlineAt = System.currentTimeMillis() + electionTimeoutMillis;

        List<Integer> higherIds = new ArrayList<>();
        for (Integer peerId : peers.keySet()) {
            if (peerId > id) {
                higherIds.add(peerId);
            }
        }

        if (higherIds.isEmpty()) {
            becomeLeaderLocked();
            return;
        }

        for (Integer peerId : higherIds) {
            send(peerId, ClusterMessage.elect(id));
        }
    }

    private void checkElectionTimeout() {
        stateLock.lock();
        try {
            if (!electionInProgress) {
                return;
            }
            if (System.currentTimeMillis() < electionDeadlineAt) {
                return;
            }
            if (receivedElectAnswer) {
                startElectionLocked();
                return;
            }
            becomeLeaderLocked();
        } finally {
            stateLock.unlock();
        }
    }

    private void becomeLeaderLocked() {
        if (!alive) {
            return;
        }
        currentLeaderId = id;
        currentRole = NodeRole.LEADER;
        electionInProgress = false;
        receivedElectAnswer = false;
        waitingPingAnswer = false;
        broadcast(ClusterMessage.victory(id));
    }

    private void send(int targetId, ClusterMessage message) {
        ClusterNode target = peers.get(targetId);
        if (target != null) {
            target.deliver(message);
        }
    }

    private void broadcast(ClusterMessage message) {
        for (ClusterNode peer : peers.values()) {
            peer.deliver(message);
        }
    }

    private void maybeSimulateFailure() {
        stateLock.lock();
        try {
            if (!alive || failProbability <= 0.0d) {
                return;
            }
            if (ThreadLocalRandom.current().nextDouble() < failProbability) {
                markDown(false);
            }
        } finally {
            stateLock.unlock();
        }
    }

    private void maybeAutoRecover() {
        stateLock.lock();
        try {
            if (alive || forceStopped) {
                return;
            }
            if (recoverAt > 0L && System.currentTimeMillis() >= recoverAt) {
                forceUp();
            }
        } finally {
            stateLock.unlock();
        }
    }

    private long randomRecoverDelay() {
        if (maxRecoverMillis <= minRecoverMillis) {
            return minRecoverMillis;
        }
        return ThreadLocalRandom.current().nextLong(minRecoverMillis, maxRecoverMillis + 1);
    }
}
