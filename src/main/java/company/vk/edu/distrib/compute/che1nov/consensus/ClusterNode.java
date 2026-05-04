package company.vk.edu.distrib.compute.che1nov.consensus;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClusterNode extends Thread {
    private final int nodeId;
    private final BlockingQueue<ClusterMessage> inbox = new LinkedBlockingQueue<>();
    private final Map<Integer, ClusterNode> peers = new ConcurrentHashMap<>();

    private final long pingIntervalMillis;
    private final long pingTimeoutMillis;
    private final long electionTimeoutMillis;

    private final double failProbability;
    private final long minRecoverMillis;
    private final long maxRecoverMillis;

    private final AtomicBoolean running = new AtomicBoolean(true);

    private boolean alive = true;
    private NodeRole role = NodeRole.FOLLOWER;
    private Integer leaderId;

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
        this.nodeId = nodeId;
        this.pingIntervalMillis = pingInterval.toMillis();
        this.pingTimeoutMillis = pingTimeout.toMillis();
        this.electionTimeoutMillis = electionTimeout.toMillis();
        this.failProbability = failProbability;
        this.minRecoverMillis = minRecover.toMillis();
        this.maxRecoverMillis = maxRecover.toMillis();
        this.nextPingAt = System.currentTimeMillis() + pingIntervalMillis;
    }

    public int nodeId() {
        return nodeId;
    }

    public synchronized Integer leaderId() {
        return leaderId;
    }

    public synchronized NodeRole role() {
        return role;
    }

    public synchronized boolean isAliveNode() {
        return alive;
    }

    public void setPeers(Map<Integer, ClusterNode> allNodes) {
        peers.clear();
        for (Map.Entry<Integer, ClusterNode> entry : allNodes.entrySet()) {
            if (entry.getKey() != nodeId) {
                peers.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public void deliver(ClusterMessage message) {
        inbox.offer(message);
    }

    public synchronized void forceDown() {
        markDown(true);
    }

    private synchronized void markDown(boolean forced) {
        if (!alive) {
            return;
        }
        forceStopped = forced;
        alive = false;
        role = NodeRole.DOWN;
        leaderId = null;
        electionInProgress = false;
        waitingPingAnswer = false;
        recoverAt = forced ? 0L : System.currentTimeMillis() + randomRecoverDelay();
    }

    public synchronized void forceUp() {
        if (alive) {
            return;
        }
        forceStopped = false;
        alive = true;
        role = NodeRole.FOLLOWER;
        leaderId = null;
        waitingPingAnswer = false;
        startElection();
    }

    public void shutdownNode() {
        running.set(false);
        this.interrupt();
    }

    @Override
    public void run() {
        startElection();
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
        if (!alive) {
            return;
        }
        send(fromId, ClusterMessage.answer(nodeId, AnswerKind.PING));
    }

    private void onElect(int fromId) {
        if (!alive) {
            return;
        }
        if (nodeId > fromId) {
            send(fromId, ClusterMessage.answer(nodeId, AnswerKind.ELECT));
            startElection();
        }
    }

    private void onAnswer(int fromId, AnswerKind answerKind) {
        if (!alive) {
            return;
        }
        if (answerKind == AnswerKind.PING && Objects.equals(leaderId, fromId)) {
            waitingPingAnswer = false;
        }
        if (answerKind == AnswerKind.ELECT && electionInProgress) {
            receivedElectAnswer = true;
            electionDeadlineAt = System.currentTimeMillis() + electionTimeoutMillis;
        }
    }

    private void onVictory(int winnerId) {
        if (!alive) {
            return;
        }
        if (winnerId < nodeId) {
            startElection();
            return;
        }
        leaderId = winnerId;
        electionInProgress = false;
        receivedElectAnswer = false;
        waitingPingAnswer = false;
        role = winnerId == nodeId ? NodeRole.LEADER : NodeRole.FOLLOWER;
    }

    private void maybePingLeader() {
        long now = System.currentTimeMillis();
        if (now < nextPingAt) {
            return;
        }
        nextPingAt = now + pingIntervalMillis;

        if (role == NodeRole.LEADER) {
            leaderId = nodeId;
            return;
        }

        if (leaderId == null) {
            startElection();
            return;
        }
        if (waitingPingAnswer) {
            return;
        }

        send(leaderId, ClusterMessage.ping(nodeId));
        waitingPingAnswer = true;
        pingDeadlineAt = now + pingTimeoutMillis;
    }

    private void checkPingTimeout() {
        if (!waitingPingAnswer) {
            return;
        }
        if (System.currentTimeMillis() >= pingDeadlineAt) {
            waitingPingAnswer = false;
            startElection();
        }
    }

    private synchronized void startElection() {
        if (!alive) {
            return;
        }

        electionInProgress = true;
        receivedElectAnswer = false;
        role = NodeRole.CANDIDATE;
        electionDeadlineAt = System.currentTimeMillis() + electionTimeoutMillis;

        List<Integer> higherIds = new ArrayList<>();
        for (Integer peerId : peers.keySet()) {
            if (peerId > nodeId) {
                higherIds.add(peerId);
            }
        }

        if (higherIds.isEmpty()) {
            becomeLeader();
            return;
        }

        for (Integer peerId : higherIds) {
            send(peerId, ClusterMessage.elect(nodeId));
        }
    }

    private void checkElectionTimeout() {
        if (!electionInProgress) {
            return;
        }
        if (System.currentTimeMillis() < electionDeadlineAt) {
            return;
        }
        if (receivedElectAnswer) {
            startElection();
            return;
        }
        becomeLeader();
    }

    private synchronized void becomeLeader() {
        if (!alive) {
            return;
        }
        leaderId = nodeId;
        role = NodeRole.LEADER;
        electionInProgress = false;
        receivedElectAnswer = false;
        waitingPingAnswer = false;
        broadcast(ClusterMessage.victory(nodeId));
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
        if (!alive || failProbability <= 0.0d) {
            return;
        }
        if (ThreadLocalRandom.current().nextDouble() < failProbability) {
            markDown(false);
        }
    }

    private void maybeAutoRecover() {
        if (alive || forceStopped) {
            return;
        }
        if (recoverAt > 0L && System.currentTimeMillis() >= recoverAt) {
            forceUp();
        }
    }

    private long randomRecoverDelay() {
        if (maxRecoverMillis <= minRecoverMillis) {
            return minRecoverMillis;
        }
        return ThreadLocalRandom.current().nextLong(minRecoverMillis, maxRecoverMillis + 1);
    }
}
