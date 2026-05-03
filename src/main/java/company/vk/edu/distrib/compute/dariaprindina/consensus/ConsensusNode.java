package company.vk.edu.distrib.compute.dariaprindina.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings({
    "PMD.AvoidUsingVolatile",
    "PMD.AvoidSynchronizedStatement"
})
public class ConsensusNode implements Runnable {
    private static final int UNKNOWN_LEADER_ID = -1;
    private static final Logger log = LoggerFactory.getLogger(ConsensusNode.class);

    private final int nodeId;
    private final ConsensusCluster cluster;
    private final List<Integer> allNodeIds;
    private final ConsensusClusterConfig config;
    private final BlockingQueue<ConsensusMessage> inbox;
    private final AtomicBoolean running;
    private final Random random;
    private final Object stateLock;

    private volatile ConsensusNodeState state;
    private volatile int knownLeaderId;
    private volatile long downUntilMillis;
    private volatile long waitForAnswerUntilMillis;
    private volatile long waitForVictoryUntilMillis;
    private volatile long nextHeartbeatAtMillis;
    private volatile boolean gotAnswerFromHigherNode;
    private Thread thread;

    public ConsensusNode(
        int nodeId,
        ConsensusCluster cluster,
        List<Integer> allNodeIds,
        ConsensusClusterConfig config
    ) {
        this.nodeId = nodeId;
        this.cluster = Objects.requireNonNull(cluster, "cluster");
        this.allNodeIds = List.copyOf(allNodeIds);
        this.config = Objects.requireNonNull(config, "config");
        this.inbox = new LinkedBlockingQueue<>();
        this.running = new AtomicBoolean(false);
        this.random = new Random(nodeId * 17L + 101);
        this.stateLock = new Object();
        this.state = ConsensusNodeState.FOLLOWER;
        this.knownLeaderId = UNKNOWN_LEADER_ID;
    }

    public void startNodeThread() {
        synchronized (stateLock) {
            if (running.get()) {
                return;
            }
            running.set(true);
            this.thread = new Thread(this, "consensus-node-" + nodeId);
            this.thread.start();
        }
    }

    public void shutdown() {
        running.set(false);
        final Thread localThread = thread;
        if (localThread == null) {
            return;
        }
        localThread.interrupt();
        try {
            localThread.join(TimeUnit.SECONDS.toMillis(2));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public boolean enqueue(ConsensusMessage message) {
        if (!running.get() || isDownNow()) {
            return false;
        }
        return inbox.offer(message);
    }

    public void forceDown() {
        synchronized (stateLock) {
            state = ConsensusNodeState.DOWN;
            knownLeaderId = UNKNOWN_LEADER_ID;
            downUntilMillis = Long.MAX_VALUE;
            resetElectionFlags();
        }
    }

    public void forceUp() {
        synchronized (stateLock) {
            downUntilMillis = 0;
            if (state == ConsensusNodeState.DOWN) {
                state = ConsensusNodeState.FOLLOWER;
            }
            beginElection("node force-up");
        }
    }

    public ConsensusNodeSnapshot snapshot() {
        final Integer leader = knownLeaderId == UNKNOWN_LEADER_ID ? null : knownLeaderId;
        return new ConsensusNodeSnapshot(nodeId, state, leader);
    }

    @Override
    public void run() {
        beginElection("cluster startup");
        while (running.get()) {
            try {
                final ConsensusMessage message = inbox.poll(100, TimeUnit.MILLISECONDS);
                if (message != null) {
                    onMessage(message);
                }
                tick();
            } catch (InterruptedException e) {
                if (!running.get()) {
                    Thread.currentThread().interrupt();
                    return;
                }
            } catch (RuntimeException e) {
                log.error("Node {} internal error", nodeId, e);
            }
        }
    }

    private void onMessage(ConsensusMessage message) {
        if (isDownNow()) {
            return;
        }
        switch (message.type()) {
            case PING -> cluster.send(message.senderId(), new ConsensusMessage(ConsensusMessageType.ANSWER, nodeId));
            case ELECT -> onElect(message.senderId());
            case ANSWER -> onAnswer(message.senderId());
            case VICTORY -> onVictory(message.senderId());
        }
    }

    private void onElect(int senderId) {
        synchronized (stateLock) {
            if (senderId < nodeId) {
                cluster.send(senderId, new ConsensusMessage(ConsensusMessageType.ANSWER, nodeId));
                beginElection("got ELECT from smaller id=" + senderId);
            }
        }
    }

    private void onAnswer(int senderId) {
        synchronized (stateLock) {
            if (senderId > nodeId && waitForAnswerUntilMillis > 0) {
                gotAnswerFromHigherNode = true;
                waitForAnswerUntilMillis = 0;
                waitForVictoryUntilMillis = System.currentTimeMillis() + config.answerTimeout().toMillis();
            }
        }
    }

    private void onVictory(int leaderId) {
        synchronized (stateLock) {
            knownLeaderId = leaderId;
            state = leaderId == nodeId ? ConsensusNodeState.LEADER : ConsensusNodeState.FOLLOWER;
            resetElectionFlags();
            nextHeartbeatAtMillis = System.currentTimeMillis() + config.heartbeatInterval().toMillis();
        }
    }

    private void tick() {
        maybeRecover();
        if (isDownNow()) {
            return;
        }
        maybeFailRandomly();
        if (isDownNow()) {
            return;
        }
        evaluateElectionTimeouts();
        heartbeatLeaderIfNeeded();
    }

    private void evaluateElectionTimeouts() {
        synchronized (stateLock) {
            final long now = System.currentTimeMillis();
            if (waitForAnswerUntilMillis > 0 && now >= waitForAnswerUntilMillis) {
                waitForAnswerUntilMillis = 0;
                if (!gotAnswerFromHigherNode) {
                    becomeLeader();
                    return;
                }
                waitForVictoryUntilMillis = now + config.answerTimeout().toMillis();
                return;
            }
            if (waitForVictoryUntilMillis > 0 && now >= waitForVictoryUntilMillis) {
                waitForVictoryUntilMillis = 0;
                beginElection("victory timeout");
            }
        }
    }

    private void heartbeatLeaderIfNeeded() {
        synchronized (stateLock) {
            if (state == ConsensusNodeState.LEADER) {
                return;
            }
            final long now = System.currentTimeMillis();
            if (now < nextHeartbeatAtMillis) {
                return;
            }
            nextHeartbeatAtMillis = now + config.heartbeatInterval().toMillis();

            final int leader = knownLeaderId;
            if (leader == UNKNOWN_LEADER_ID || leader == nodeId) {
                beginElection("leader unknown");
                return;
            }
            final boolean answered = cluster.send(leader, new ConsensusMessage(ConsensusMessageType.PING, nodeId));
            if (!answered) {
                beginElection("leader ping failed");
            }
        }
    }

    private void beginElection(String reason) {
        synchronized (stateLock) {
            if (isDownNow()) {
                return;
            }
            log.info("Node {} starts election: {}", nodeId, reason);
            state = ConsensusNodeState.FOLLOWER;
            knownLeaderId = UNKNOWN_LEADER_ID;
            gotAnswerFromHigherNode = false;

            boolean hasHigherCandidate = false;
            final ConsensusMessage electMessage = new ConsensusMessage(ConsensusMessageType.ELECT, nodeId);
            for (Integer peerId : allNodeIds) {
                if (peerId <= nodeId) {
                    continue;
                }
                final boolean sent = cluster.send(peerId, electMessage);
                hasHigherCandidate = hasHigherCandidate || sent;
            }
            if (!hasHigherCandidate) {
                becomeLeader();
                return;
            }
            waitForAnswerUntilMillis = System.currentTimeMillis() + config.answerTimeout().toMillis();
            waitForVictoryUntilMillis = 0;
        }
    }

    private void becomeLeader() {
        synchronized (stateLock) {
            if (isDownNow()) {
                return;
            }
            state = ConsensusNodeState.LEADER;
            knownLeaderId = nodeId;
            resetElectionFlags();
            cluster.broadcast(new ConsensusMessage(ConsensusMessageType.VICTORY, nodeId));
            log.info("Node {} became leader", nodeId);
        }
    }

    private void maybeFailRandomly() {
        synchronized (stateLock) {
            if (config.failureProbabilityPerTick() <= 0) {
                return;
            }
            if (random.nextDouble() >= config.failureProbabilityPerTick()) {
                return;
            }
            state = ConsensusNodeState.DOWN;
            knownLeaderId = UNKNOWN_LEADER_ID;
            downUntilMillis = System.currentTimeMillis() + config.restoreDelay().toMillis();
            resetElectionFlags();
            log.info("Node {} failed, down until {}", nodeId, downUntilMillis);
        }
    }

    private void maybeRecover() {
        synchronized (stateLock) {
            if (state != ConsensusNodeState.DOWN) {
                return;
            }
            if (downUntilMillis == Long.MAX_VALUE) {
                return;
            }
            if (System.currentTimeMillis() < downUntilMillis) {
                return;
            }
            state = ConsensusNodeState.FOLLOWER;
            knownLeaderId = UNKNOWN_LEADER_ID;
            downUntilMillis = 0;
            log.info("Node {} restored", nodeId);
            beginElection("node recovered");
        }
    }

    private boolean isDownNow() {
        return state == ConsensusNodeState.DOWN;
    }

    private void resetElectionFlags() {
        waitForAnswerUntilMillis = 0;
        waitForVictoryUntilMillis = 0;
        gotAnswerFromHigherNode = false;
    }
}
