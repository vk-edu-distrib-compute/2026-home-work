package company.vk.edu.distrib.compute.ronshinvsevolod.consensus;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ScheduledFuture;
import java.util.List;
import java.util.logging.Logger;

public final class Node implements Runnable {

    private static final Logger LOG = Logger.getLogger(Node.class.getName());
    private static final int QUEUE_TIMEOUT_MS = 100;
    private static final int PING_INTERVAL_MS = 500;
    private static final int ELECTION_TIMEOUT_MS = QUEUE_TIMEOUT_MS * 3;
    private static final String NODE_PREFIX = "[Node ";
    private final int nodeId;
    private final AtomicReference<NodeState> state;
    private final AtomicReference<Thread> nodeThread;
    private final AtomicReference<ScheduledFuture<?>> leaderTimeout;
    private final AtomicInteger leaderId;
    private final AtomicBoolean running;
    private final AtomicBoolean receivedAnswer;
    private final BlockingQueue<Message> inbox;
    private final ScheduledExecutorService scheduler;
    private final AtomicReference<ScheduledFuture<?>> pendingElection;
    private List<Node> peers;

    public Node(final int nodeId) {
        this.nodeId = nodeId;
        this.inbox = new LinkedBlockingQueue<>();
        this.state = new AtomicReference<>(NodeState.FOLLOWER);
        this.leaderId = new AtomicInteger(-1);
        this.running = new AtomicBoolean(false);
        this.receivedAnswer = new AtomicBoolean(false);
        this.leaderTimeout = new AtomicReference<>(null);
        this.nodeThread = new AtomicReference<>(null);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
            rn -> {
                final Thread th = new Thread(rn, "election-scheduler-" + nodeId);
                th.setDaemon(true);
                return th;
            }
        );
        this.pendingElection = new AtomicReference<>(null);
    }

    public int getNodeId() {
        return nodeId;
    }

    public NodeState getState() {
        return state.get();
    }

    public int getLeaderId() {
        return leaderId.get();
    }

    static int getElectionSettleMs() {
        return PING_INTERVAL_MS * 3 / 2 + 2 * ELECTION_TIMEOUT_MS + QUEUE_TIMEOUT_MS;
    }

    public void setPeers(final List<Node> peerList) {
        this.peers = peerList;
    }

    public void start() {
        running.set(true);
        final Thread thread = new Thread(this, "node-" + nodeId);
        thread.setDaemon(true);
        nodeThread.set(thread);
        thread.start();
        triggerInitialElection();
    }

    private void triggerInitialElection() {
        final ScheduledFuture<?> future = scheduler.schedule(
            () -> {
                if (state.get() == NodeState.FOLLOWER && leaderId.get() < 0) {
                    startElection();
                }
            },
            QUEUE_TIMEOUT_MS,
            TimeUnit.MILLISECONDS
        );
        leaderTimeout.set(future);
    }

    public void stop() {
        running.set(false);
        scheduler.shutdownNow();
        final Thread thread = nodeThread.get();
        if (thread != null) {
            thread.interrupt();
        }
    }

    public void fail() {
        if (LOG.isLoggable(java.util.logging.Level.INFO)) {
            LOG.info(NODE_PREFIX + nodeId + "] FAILED");
        }
        state.set(NodeState.FAILED);
    }

    public void recover() {
        if (LOG.isLoggable(java.util.logging.Level.INFO)) {
            LOG.info(NODE_PREFIX + nodeId + "] RECOVERED - starting election");
        }
        state.set(NodeState.FOLLOWER);
        leaderId.set(-1);
        inbox.clear();
        startElection();
    }

    public void receive(final Message msg) {
        if (state.get() != NodeState.FAILED) {
            inbox.offer(msg);
        }
    }

    @Override
    public void run() {
        while (running.get()) {
            try {
                tick();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void tick() throws InterruptedException {
        if (state.get() == NodeState.FAILED) {
            TimeUnit.MILLISECONDS.sleep(QUEUE_TIMEOUT_MS);
            return;
        }

        final Message msg = inbox.poll(QUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        if (msg != null) {
            handleMessage(msg);
        }

        if (state.get() == NodeState.LEADER) {
            pingFollowers();
            TimeUnit.MILLISECONDS.sleep(PING_INTERVAL_MS);
        }
    }

    private void handleMessage(final Message msg) {
        switch (msg.getType()) {
            case ELECT:
                handleElect(msg);
                break;
            case ANSWER:
                receivedAnswer.set(true);
                break;
            case VICTORY:
                handleVictory(msg);
                break;
            case PING:
                handlePing(msg);
                break;
        }
    }

    private void handleElect(final Message msg) {
        send(msg.getSenderId(), MessageType.ANSWER);
        if (state.get() != NodeState.CANDIDATE && state.get() != NodeState.LEADER) {
            leaderId.set(-1);
            startElection();
        }
    }

    private void resetLeaderTimeout() {
        final ScheduledFuture<?> prev = leaderTimeout.get();
        if (prev != null) {
            prev.cancel(false);
        }
        final ScheduledFuture<?> future = scheduler.schedule(
            () -> {
                if (state.get() == NodeState.FOLLOWER) {
                    leaderId.set(-1);
                    startElection();
                }
            },
            PING_INTERVAL_MS * 3 / 2,
            TimeUnit.MILLISECONDS
        );
        leaderTimeout.set(future);
    }

    private void handleVictory(final Message msg) {
        leaderId.set(msg.getSenderId());
        state.set(NodeState.FOLLOWER);
        if (LOG.isLoggable(java.util.logging.Level.INFO)) {
            LOG.info(NODE_PREFIX + nodeId + "] acknowledges leader: " + msg.getSenderId());
        }
        resetLeaderTimeout();
    }

    private void handlePing(final Message msg) {
        send(msg.getSenderId(), MessageType.ANSWER);
        resetLeaderTimeout();
    }

    private void startElection() {
        receivedAnswer.set(false);
        if (state.get() == NodeState.FAILED) {
            return;
        }
        state.set(NodeState.CANDIDATE);
        leaderId.set(-1);
        if (LOG.isLoggable(java.util.logging.Level.INFO)) {
            LOG.info(NODE_PREFIX + nodeId + "] starting election");
        }
        if (!broadcastElect()) {
            declareVictory();
            return;
        }
        scheduleElectionFuture();
    }

    private boolean broadcastElect() {
        final Message electMsg = new Message(MessageType.ELECT, nodeId);
        final boolean higherAlive = peers.stream()
            .anyMatch(nd -> nd.getNodeId() > nodeId && nd.getState() != NodeState.FAILED);
        peers.stream()
            .filter(nd -> nd.getNodeId() > nodeId && nd.getState() != NodeState.FAILED)
            .forEach(nd -> nd.receive(electMsg));
        return higherAlive;
    }

    private void scheduleElectionFuture() {
        final ScheduledFuture<?> prev = pendingElection.get();
        if (prev != null) {
            prev.cancel(false);
        }
        final ScheduledFuture<?> future = scheduler.schedule(
            () -> {
                if (state.get() == NodeState.CANDIDATE && !receivedAnswer.get()) {
                    declareVictory();
                }
            },
            ELECTION_TIMEOUT_MS,
            TimeUnit.MILLISECONDS
        );
        pendingElection.set(future);
    }

    private void declareVictory() {
        state.set(NodeState.LEADER);
        leaderId.set(nodeId);
        if (LOG.isLoggable(java.util.logging.Level.INFO)) {
            LOG.info(NODE_PREFIX + nodeId + "] declares VICTORY - is new leader");
        }
        final Message victoryMsg = new Message(MessageType.VICTORY, nodeId);
        for (final Node nd : peers) {
            if (nd.getState() != NodeState.FAILED) {
                nd.receive(victoryMsg);
            }
        }
    }

    private void pingFollowers() {
        final Message pingMsg = new Message(MessageType.PING, nodeId);
        for (final Node nd : peers) {
            if (nd.getState() != NodeState.FAILED) {
                nd.receive(pingMsg);
            }
        }
    }

    private void send(final int targetId, final MessageType msgType) {
        peers.stream()
            .filter(nd -> nd.getNodeId() == targetId)
            .findFirst()
            .ifPresent(nd -> nd.receive(new Message(msgType, nodeId)));
    }
}
